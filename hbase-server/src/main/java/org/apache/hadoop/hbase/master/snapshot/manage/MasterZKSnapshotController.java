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
package org.apache.hadoop.hbase.master.snapshot.manage;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.server.snapshot.ZKSnapshotController;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * Controller between zookeeper and the snapshot manager for the master.
 * <p>
 * Takes care of the nuts and bolts of starting/stoping/etc snapshots
 */
public class MasterZKSnapshotController extends ZKSnapshotController {

  public static final Log LOG = LogFactory.getLog(MasterZKSnapshotController.class);
  private final SnapshotManager manager;

  public MasterZKSnapshotController(final ZooKeeperWatcher watcher, final SnapshotManager manager)
      throws KeeperException {
    // If the cluster was shutdown mid-snapshot, then we are going to lose
    // a snapshot that was previously started. Any snapshot previously running will be aborted
    super(watcher);
    this.manager = manager;

    // and now cleanup everything below the snapshots below
    ZKUtil.deleteChildrenRecursively(watcher, startSnapshotBarrier);
    ZKUtil.deleteChildrenRecursively(watcher, endSnapshotBarrier);
    ZKUtil.deleteChildrenRecursively(watcher, abortZnode);

  }

  @Override
  public void nodeChildrenChanged(String path) {
    if (!path.startsWith(watcher.snapshotZNode)) return;
    try {

      LOG.debug("Children changed for node:" + path);
      LOG.debug("Current start tree:");
      logZKTree(startSnapshotBarrier);
      if (path.startsWith(startSnapshotBarrier)) {
        childrenJoinedSnapshot(ZKUtil.listChildrenAndWatchForNewChildren(watcher, path));
      } else if (path.startsWith(endSnapshotBarrier)) {
        childrenCompletedSnapshot(ZKUtil.listChildrenAndWatchForNewChildren(watcher, path));
      } else if (path.startsWith(abortZnode)) {
        for (String child : ZKUtil.listChildrenAndWatchForNewChildren(watcher, path)) {
          this.manager.rsAbortedSnapshot(child);
        }
      }
    } catch (KeeperException e) {
      // simple catch-all incase of losing zk connection
      throw new RuntimeException(e);
    }
  }

  @Override
  public void nodeCreated(String path) {
    if (!path.startsWith(watcher.snapshotZNode)) return;
    // only node that can be created that we care about could be the abort node
    try {
      LOG.debug("Node created: " + path);
      if (path.startsWith(abortZnode)) {
        for (String child : ZKUtil.listChildrenNoWatch(watcher, path)) {
          this.manager.rsAbortedSnapshot(child);
        }
      }
    } catch (KeeperException e) {
      // simple catch-all incase of losing zk connection
      throw new RuntimeException(e);
    }
  }

  /**
   * Start a snapshot for the given table descriptor.
   * <p>
   * Simply update the info for the start znode, indicating a snapshot is starting.
   * @param regions regions involved in this snapshot
   * @param sd {@link SnapshotDescriptor} of the snapshot to create
   * @throws IOException on unexpected error
   */
  void startSnapshot(SnapshotDescriptor sd) throws KeeperException, IOException {
    // start watching for the abort node
    String abortNode = getSnapshotAbortNode(sd);
    if (ZKUtil.watchAndCheckExists(watcher, abortNode)) {
      throw new SnapshotCreationException("Snapshot already aborted before created.");
    }

    // create the start barrier
    String snapshotNode = getSnapshotStartBarrierNode(sd);
    ZKUtil.createSetData(watcher, snapshotNode, Writables.getBytes(sd));

    // start watching for servers that joined, and update with any that are fast
    List<String> children = ZKUtil.listChildrenAndWatchForNewChildren(watcher, snapshotNode);
    childrenJoinedSnapshot(children);
  }

  /**
   * Notify the manager of all the children that have joined/prepared the snapshot
   */
  private void childrenJoinedSnapshot(List<String> children) {
    if (children == null) return;
    for (String child : children) {
      LOG.debug("Node:" + child + " has prepared snapshot.");
      this.manager.rsJoinedSnapshot(child);
    }
  }

  /**
   * Post the 'commit' snapshot node so watchers will commit the snapshot
   * @param sd snapshot to complete
   * @throws KeeperException
   */
  void completeSnapshot(SnapshotDescriptor sd) throws KeeperException {
    String snapshotNode = getSnapshotEndBarrierNode(sd);
    LOG.debug("Creating zk node:" + snapshotNode);
    ZKUtil.createAndFailSilent(watcher, snapshotNode);
    childrenCompletedSnapshot(ZKUtil.listChildrenAndWatchForNewChildren(watcher, snapshotNode));
  }

  /**
   * Notify the manager of all the children that have completed/committed the snapshot
   */
  private void childrenCompletedSnapshot(List<String> children) {
    if (children == null) return;
    for (String child : children) {
      LOG.debug("Node:" + child + " has completed snapshot.");
      this.manager.rsCompletedSnapshot(child);
    }
  }

  /**
   * Remove the zookeeper nodes associated with a given snaphot, freeing it to
   * be used again
   * @param snapshot description of the snapshot to reset
   * @throws KeeperException if an unexpected exception occurs
   */
  public void resetZooKeeper(SnapshotDescriptor snapshot) throws KeeperException {
    ZKUtil.deleteNodeRecursively(watcher, this.getSnapshotAbortNode(snapshot));
    ZKUtil.deleteNodeRecursively(watcher, this.getSnapshotEndBarrierNode(snapshot));
    ZKUtil.deleteNodeRecursively(watcher, this.getSnapshotStartBarrierNode(snapshot));

  }

  private String getSnapshotStartBarrierNode(SnapshotDescriptor snapshot) {
    return ZKUtil.joinZNode(startSnapshotBarrier, snapshot.getSnapshotNameAsString());
  }

  private String getSnapshotEndBarrierNode(SnapshotDescriptor snapshot) {
    return ZKUtil.joinZNode(endSnapshotBarrier, snapshot.getSnapshotNameAsString());
  }

  private String getSnapshotAbortNode(SnapshotDescriptor snapshot) {
    return ZKUtil.joinZNode(abortZnode, snapshot.getSnapshotNameAsString());
  }

  //--------------------------------------------------------------------------
  //internal debugging methods
  //--------------------------------------------------------------------------
  /**
   * Recursively print the current state of ZK (non-transactional)
   * @param root name of the root directory in zk to print
   * @throws KeeperException
   */
  private void logZKTree(String root) throws KeeperException {
    if (!LOG.isDebugEnabled()) return;
    LOG.debug("Current zk system:");
    logFSTree(root, "|-");
  }

  /**
   * Helper method to print the current state of the ZK tree.
   * @see #logZKTree(String)
   * @throws KeeperException if an unexpected exception occurs
   */
  private void logFSTree(String root, String prefix) throws KeeperException {
    List<String> children = ZKUtil.listChildrenNoWatch(watcher, root);
    if (children == null) return;
    for (String child : children) {
      LOG.debug(prefix + child + "/");
      logFSTree(ZKUtil.joinZNode(root, child), prefix + "---");
    }
  }
}
