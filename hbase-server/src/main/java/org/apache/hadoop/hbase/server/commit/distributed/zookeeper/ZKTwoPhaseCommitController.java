/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.server.commit.distributed.zookeeper;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.generated.ErrorHandlingProtos.RemoteFailureException;
import org.apache.hadoop.hbase.server.commit.distributed.DistributedThreePhaseCommitManager;
import org.apache.hadoop.hbase.server.commit.distributed.controller.DistributedCommitController;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * ZooKeeper based controller for a distributed two-phase commit.
 * <p>
 * {@link #close()} does not do anything and need not be called on <tt>this</tt>
 * @param <L> type of listener to watch for errors and progress of an operation (both local and
 *          remote)
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ZKTwoPhaseCommitController<L extends DistributedThreePhaseCommitManager<?, ?, ?>>
    extends ZooKeeperListener implements DistributedCommitController<L> {

  private static final Log LOG = LogFactory.getLog(ZKTwoPhaseCommitController.class);

  public static final String START_BARRIER_ZNODE = "prepare";
  public static final String END_BARRIER_ZNODE = "commit";
  public static final String ABORT_ZNODE = "abort";

  public final String baseZNode;
  protected final String prepareBarrier;
  protected final String commitBarrier;
  protected final String abortZnode;

  protected final String nodeName;
  protected L listener;

  /*
   * Layout of zk is
   * /hbase/[op name]/prepare/
   *                    [op instance] - op data/
   *                        /[nodes that have prepared]
   *                 /commit/
   *                    [op instance]/
   *                        /[nodes that have committed]
   *                /abort/
   *                    [op instance] - failure data/
   *  Assumption here that snapshot names are going to be unique
   */

  /**
   * Top-level watcher/controller for snapshots across the cluster.
   * <p>
   * On instantiation ensures the snapshot znodes exists, but requires calling {@link #start} to
   * start monitoring for running two phase commits.
   * @param watcher watcher for the zookeeper cluster - not managed by <tt>this</tt>- must be
   *          externally closed.
   * @param operationDescription name of the znode describing the operation to run
   * @param nodeName name of the node from which we are interacting with running operations
   * @throws KeeperException when the operation znodes cannot be created
   */
  public ZKTwoPhaseCommitController(ZooKeeperWatcher watcher,
      String operationDescription,
      String nodeName) throws KeeperException {
    super(watcher);
    this.nodeName = nodeName;

    // make sure we are listening for events
    watcher.registerListener(this);
    // setup paths for the zknodes used in snapshotting
    this.baseZNode = ZKUtil.joinZNode(watcher.baseZNode, operationDescription);
    prepareBarrier = ZKUtil.joinZNode(baseZNode, START_BARRIER_ZNODE);
    commitBarrier = ZKUtil.joinZNode(baseZNode, END_BARRIER_ZNODE);
    abortZnode = ZKUtil.joinZNode(baseZNode, ABORT_ZNODE);

    // first make sure all the ZK nodes exist
    // make sure all the parents exist (sometimes not the case in tests)
    ZKUtil.createWithParents(watcher, prepareBarrier);
    // regular create because all the parents exist
    ZKUtil.createAndFailSilent(watcher, commitBarrier);
    ZKUtil.createAndFailSilent(watcher, abortZnode);
  }

  @Override
  public final void start(L listener) {
    LOG.debug("Starting the commit controller for operation:" + this.nodeName);
    this.listener = listener;
    this.start();
  }

  /**
   * Start monitoring nodes in ZK - subclass hook to start monitoring nodes they are about.
   */
  protected void start() {
    // NOOP - used by subclasses to start monitoring things they care about
  }

  @Override
  public void close() throws IOException {
    // noop
  }

  @Override
  public void abortOperation(String operationName, RemoteFailureException failureInfo) {
    LOG.debug("Aborting operation (" + operationName + ") in zk");
    String operationAbortNode = getAbortNode(this, operationName);
    try {
      LOG.debug("Creating abort node:" + operationAbortNode);
      byte[] errorInfo = failureInfo.toByteArray();
      // first create the znode for the operation
      ZKUtil.createSetData(watcher, operationAbortNode, errorInfo);
      LOG.debug("Finished creating abort node:" + operationAbortNode);
    } catch (KeeperException e) {
      // possible that we get this error for the operation if we already reset the zk state, but in
      // that case we should still get an error for that operation anyways
      logZKTree(this.baseZNode);
      listener.controllerConnectionFailure("Failed to post zk node:" + operationAbortNode
          + " to abort operation", new IOException(e));
    }
  }

  /**
   * Get the full znode path to the node used by the coordinator to starting the operation and as a
   * barrier node for the prepare phase.
   * @param controller controller running the operation
   * @param opInstanceName name of the running operation instance (not the operation description).
   * @return full znode path to the prepare barrier/start node
   */
  public static String getPrepareBarrierNode(ZKTwoPhaseCommitController<?> controller,
      String opInstanceName) {
    return ZKUtil.joinZNode(controller.prepareBarrier, opInstanceName);
  }

  /**
   * Get the full znode path to the node used by the coordinator as a barrier for the commit phase.
   * @param controller controller running the operation
   * @param opInstanceName name of the running operation instance (not the operation description).
   * @return full znode path to the commit barrier
   */
  public static String getCommitBarrierNode(ZKTwoPhaseCommitController<?> controller,
      String opInstanceName) {
    return ZKUtil.joinZNode(controller.commitBarrier, opInstanceName);
  }

  /**
   * Get the full znode path to the the node that will be created if an operation fails on any node
   * @param controller controller running the operation
   * @param opInstanceName name of the running operation instance (not the operation description).
   * @return full znode path to the abort znode
   */
  public static String getAbortNode(ZKTwoPhaseCommitController<?> controller, String opInstanceName) {
    return ZKUtil.joinZNode(controller.abortZnode, opInstanceName);
  }

  /**
   * Pass along the found abort notification to the listener
   * @param abortNode full znode path to the failed operation information
   */
  protected void abort(String abortNode) {
    String opName = ZKUtil.getNodeName(abortNode);
    try {
      byte[] data = ZKUtil.getData(watcher, abortNode);
      this.listener.abortOperation(opName, data);
    } catch (KeeperException e) {
      listener.controllerConnectionFailure("Failed to get data for abort node:" + abortNode
          + this.abortZnode, new IOException(e));
    }
  }

  // --------------------------------------------------------------------------
  // internal debugging methods
  // --------------------------------------------------------------------------
  /**
   * Recursively print the current state of ZK (non-transactional)
   * @param root name of the root directory in zk to print
   * @throws KeeperException
   */
  protected void logZKTree(String root) {
    if (!LOG.isDebugEnabled()) return;
    LOG.debug("Current zk system:");
    String prefix = "|-";
    LOG.debug(prefix + root);
    try {
      logZKTree(root, prefix);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Helper method to print the current state of the ZK tree.
   * @see #logZKTree(String)
   * @throws KeeperException if an unexpected exception occurs
   */
  protected void logZKTree(String root, String prefix) throws KeeperException {
    List<String> children = ZKUtil.listChildrenNoWatch(watcher, root);
    if (children == null) return;
    for (String child : children) {
      LOG.debug(prefix + child);
      String node = ZKUtil.joinZNode(root.equals("/") ? "" : root, child);
      logZKTree(node, prefix + "---");
    }
  }
}