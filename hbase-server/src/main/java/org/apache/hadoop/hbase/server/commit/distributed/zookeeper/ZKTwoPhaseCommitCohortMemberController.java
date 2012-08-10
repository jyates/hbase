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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.server.commit.distributed.cohort.DistributedCommitCohortMemberController;
import org.apache.hadoop.hbase.server.commit.distributed.cohort.DistributedThreePhaseCommitCohortMember;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * ZooKeeper based controller for a two-phase commit cohort member.
 * <p>
 * There can only be one {@link ZKTwoPhaseCommitCohortMemberController} per operation per node,
 * since each operation type is bound to a single set of nodes. You can have multiple
 * {@link ZKTwoPhaseCommitCohortMemberController} on the same server, each serving a different node
 * name, but each individual controller is still bound to a single node name (and since they are
 * used to determine global progress, its important to not get this wrong).
 * <p>
 * To make this slightly more confusing, you can run multiple, concurrent operations at the same
 * time (as long as they have different names), from the same controller, but the same node name
 * must be used for each operation (though there is no conflict between the two operations as long
 * as they have distinct names).
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ZKTwoPhaseCommitCohortMemberController extends
    ZKTwoPhaseCommitController<DistributedThreePhaseCommitCohortMember> implements
    DistributedCommitCohortMemberController {

  private static final Log LOG = LogFactory.getLog(ZKTwoPhaseCommitCohortMemberController.class);
  private final String nodeName;

  /**
   * Must call {@link #start(DistributedThreePhaseCommitCohortMember)} before this is can be used.
   * @param watcher to be used, not managed locally
   * @param operationDescription name of the znode describing the operation to run
   * @param nodeName name of the node to join the operation
   * @throws KeeperException if we can't reach zookeeper
   */
  public <T> ZKTwoPhaseCommitCohortMemberController(ZooKeeperWatcher watcher,
      String operationDescription, String nodeName) throws KeeperException {
    super(watcher, operationDescription, nodeName);
    this.nodeName = nodeName;
  }

  @Override
  protected void start() {
    LOG.debug("Starting the cohort controller");
    watchForAbortedOperations();
    watchForNewOperations();
  }

  @Override
  public void nodeCreated(String path) {
    if (path.startsWith(this.baseZNode)) {
      LOG.info("Received created event:" + path);
      // if it is a simple start/end/abort then we just rewatch the node
      if (path.equals(this.prepareBarrier)) {
        watchForNewOperations();
        return;
      } else if (path.equals(this.abortZnode)) {
        watchForAbortedOperations();
        return;
      }
      String parent = ZKUtil.getParent(path);
      // if its the end barrier, the operation can be completed
      if (parent.equals(this.commitBarrier)) {
        passAlongCommit(path);
        return;
      } else if (parent.equals(this.abortZnode)) {
        abort(path);
        return;
      } else if (parent.equals(this.prepareBarrier)) {
        startNewOperation(path);
      } else {
        LOG.debug("Ignoring created notification for node:" + path);
      }
    }
  }

  /**
   * Pass along the commit notification to any listeners
   * @param path full znode path that cause the notification
   */
  private void passAlongCommit(String path) {
    LOG.debug("Recieved committed event:" + path);
    String opName = ZKUtil.getNodeName(path);
    this.listener.commitInitiated(opName);
  }

  @Override
  public void nodeChildrenChanged(String path) {
    LOG.info("Received children changed event:" + path);
    if (path.equals(this.prepareBarrier)) {
      LOG.info("Recieved start event.");
      watchForNewOperations();
    } else if (path.equals(this.abortZnode)) {
      LOG.info("Recieved abort event.");
      watchForAbortedOperations();
    }
  }

  private void watchForAbortedOperations() {
    LOG.debug("Checking for aborted operations on node:" + this.abortZnode);
    try {
      // this is the list of the currently aborted operations
      for (String node : ZKUtil.listChildrenAndWatchForNewChildren(watcher, this.abortZnode)) {
        String abortNode = ZKUtil.joinZNode(this.abortZnode, node);
        abort(abortNode);
      }
    } catch (KeeperException e) {
      listener.controllerConnectionFailure("Failed to list children for abort node:"
          + this.abortZnode, new IOException(e));
    }
  }

  private void watchForNewOperations() {
    // watch for new operations that we need to start
    LOG.debug("Looking for new operations under znode:" + this.prepareBarrier);
    List<String> runningOperations = null;
    try {
      runningOperations = ZKUtil.listChildrenAndWatchForNewChildren(watcher, this.prepareBarrier);
      if (runningOperations == null) {
        LOG.debug("No running operations.");
        return;
      }
    } catch (KeeperException e) {
      listener.controllerConnectionFailure("General failure when watching for new operations",
        new IOException(e));
    }
    for (String operationName : runningOperations) {
      // then read in the operation information
      String path = ZKUtil.joinZNode(this.prepareBarrier, operationName);
      startNewOperation(path);
    }
  }

  /**
   * Kick off a new operation on the listener with the data stored in the passed znode.
   * <p>
   * Will attempt to create the same operation multiple times if an operation znode with the same
   * name is created. It is left up the coordinator to ensure this doesn't occur.
   * @param path full path to the znode for the operation to start
   */
  private synchronized void startNewOperation(String path) {
    LOG.debug("Found operation node: " + path);
    String opName = ZKUtil.getNodeName(path);
    // start watching for an abort notification for the operation
    String abortNode = getAbortNode(this, opName);
    try {
      if (ZKUtil.watchAndCheckExists(watcher, abortNode)) {
        LOG.debug("Not starting:" + opName + " because we already have an abort notification.");
        return;
      }
    } catch (KeeperException e) {
      listener.controllerConnectionFailure("Failed to get the abort node (" + abortNode
          + ") for operation:" + opName, new IOException(e));
      return;
    }

    // get the data for the operation
    try {
      byte[] data = ZKUtil.getData(watcher, path);
      LOG.debug("Found data for znode:" + path);
      listener.runNewOperation(opName, data);
    } catch (KeeperException e) {
      listener.controllerConnectionFailure("Failed to get data for new operation:" + opName,
        new IOException(e));
    }
  }

  @Override
  public void prepared(String operationName) throws IOException {
    try {
      LOG.debug("Node: '" + nodeName + "' joining prepared barrier for operation (" + operationName
          + ") in zk");
      String prepared = ZKUtil.joinZNode(getPrepareBarrierNode(this, operationName), nodeName);
      ZKUtil.createAndFailSilent(watcher, prepared);

      // watch for the complete node for this snapshot
      String commitBarrier = getCommitBarrierNode(this, operationName);
      LOG.debug("Starting to watch for commit barrier:" + commitBarrier);
      if (ZKUtil.watchAndCheckExists(watcher, commitBarrier)) {
        passAlongCommit(commitBarrier);
      }
    } catch (KeeperException e) {
      listener.controllerConnectionFailure("Failed to join the prepare barrier for operaton: "
          + operationName + " and node: " + nodeName, new IOException(e));
    }
  }

  @Override
  public void commited(String operationName) throws IOException {
    LOG.debug("Marking operation (" + operationName + ") committed for node '" + nodeName
        + "' in zk");
    String joinPath = ZKUtil.joinZNode(getCommitBarrierNode(this, operationName), nodeName);
    try {
      ZKUtil.createAndFailSilent(watcher, joinPath);
    } catch (KeeperException e) {
      listener.controllerConnectionFailure("Failed to post zk node:" + joinPath
          + " to join commit barrier.", new IOException(e));
    }
  }
}