package org.apache.hadoop.hbase.master.snapshot;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.server.snapshot.ZKSnapshotController;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

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
    super(watcher);
    this.manager = manager;
    // TODO: If the cluster was shutdown mid-snapshot, then we are going to lose
    // a snapshot that was previously started. This is definitely a
    // race-condition/corner case that needs to be thought about later.

    // in general this will be fine, but we need to do two things:
    // 1. If we have running snapshots then abort them (and do other necessary
    // cleanup - snapshot cleaner)
    // 2. On failure, attempt to stop snapshots

    // and now cleanup everything below the snapshots below
    // TODO this is the dangerous part of just removing the old snapshots
    ZKUtil.deleteChildrenRecursively(watcher, startSnapshotBarrier);
    ZKUtil.deleteChildrenRecursively(watcher, endSnapshotBarrier);
    ZKUtil.deleteChildrenRecursively(watcher, abortZnode);

  }

  @Override
  public void nodeChildrenChanged(String path) {
    if (!path.startsWith(watcher.snapshotZNode)) return;
    try {
      LOG.debug("Children changed for node:" + path);
      if (path.startsWith(startSnapshotBarrier)) {
        childrenJoinedSnapshot(ZKUtil.listChildrenAndWatchForNewChildren(watcher, path));
      } else if (path.startsWith(endSnapshotBarrier)) {
        childrenCompletedSnapshot(ZKUtil.listChildrenAndWatchForNewChildren(watcher, path));
      }
      // and if anything rs aborts
      else if (path.startsWith(abortZnode)) {
        for (String child : ZKUtil.listChildrenAndWatchForNewChildren(watcher, path)) {
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
   * Simply update the info for the start znode, indicating a snapshot is
   * starting.
   * 
   * @param regions regions involved in this snapshot
   * @param sd {@link SnapshotDescriptor} of the snapshot to create
   * @throws IOException
   */
  void startSnapshot(SnapshotDescriptor sd)
      throws KeeperException, IOException {
    // create the start barrier
    String snapshotNode = ZKUtil.joinZNode(startSnapshotBarrier, sd.getSnapshotNameAsString());
    ZKUtil.createSetData(watcher, snapshotNode, Writables.getBytes(sd));
    // start watching for servers that joined, and update with any that are fast
    List<String> children = ZKUtil.listChildrenAndWatchForNewChildren(watcher, snapshotNode);
    childrenJoinedSnapshot(children);
  }

  private void childrenJoinedSnapshot(List<String> children) {
    for (String child : children) {
      LOG.debug("Node:" + child + " has prepared snapshot.");
      this.manager.rsJoinedSnapshot(child);
    }
  }


  void completeSnapshot(SnapshotDescriptor sd) throws KeeperException {
    String snapshotNode = ZKUtil.joinZNode(endSnapshotBarrier, sd.getSnapshotNameAsString());
    ZKUtil.createAndFailSilent(watcher, endSnapshotBarrier);
    childrenCompletedSnapshot(ZKUtil.listChildrenAndWatchForNewChildren(watcher, snapshotNode));
  }

  private void childrenCompletedSnapshot(List<String> children) {
    for (String child : children) {
      LOG.debug("Node:" + child + " has completed snapshot.");
      this.manager.rsCompletedSnapshot(child);
    }
  }
}
