package org.apache.hadoop.hbase.regionserver.snapshot;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.snapshot.monitor.SnapshotFailureListener;
import org.apache.hadoop.hbase.server.snapshot.ZKSnapshotController;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Preconditions;

/**
 * Control the {@link HRegionServer} specific information for a snapshot.
 * <p>
 * Responds to HMaster based updated to the snapshot zk nodes and can report
 * back status as the RS snapshot progresses.
 */
public class RegionZKSnapshotController extends ZKSnapshotController implements
    SnapshotFailureListener {
  static final Log LOG = LogFactory.getLog(RegionZKSnapshotController.class);

  private final SnapshotListener listener;
  private final Server parent;
  private final Abortable abortable;

  public RegionZKSnapshotController(final ZooKeeperWatcher watcher,
      final SnapshotListener listener, final Server parent, final Abortable abortable)
      throws KeeperException {
    super(watcher);
    this.listener = listener;
    this.abortable = abortable;
    this.parent = parent;
  }

  @Override
  public void start() throws KeeperException {
    LOG.debug("Starting the region controller.");
    watchForAbortedSnapshots();
    watchForNewSnapshots();
  }

  @Override
  public void nodeChildrenChanged(String path) {
    LOG.info("Received children changed event:" + path);
    // if a new snapshot is starting
    try {
      if (path.startsWith(startSnapshotBarrier)) {
        LOG.info("Recieved start event.");
        watchForNewSnapshots();
      } else if (path.startsWith(abortZnode)) {
        LOG.info("Recieved abort event.");
        watchForAbortedSnapshots();
      }
    } catch (KeeperException e) {
      abort("Failed to update ZK with snapshot information", e);
    }
  }

  private void watchForAbortedSnapshots() throws KeeperException {
    for (String node : ZKUtil.listChildrenAndWatchForNewChildren(watcher, abortZnode)) {
      this.listener.remoteSnapshotFailure(node, "Snapshot " + node + " aborted in ZK.");
    }
  }

  private void watchForNewSnapshots() throws KeeperException {
    // watch for new snapshots that we need to join
    try {
      for (String snapshotName : ZKUtil.listChildrenAndWatchForNewChildren(watcher,
        startSnapshotBarrier)) {
        // then read in the snapshot information
        String path = ZKUtil.joinZNode(startSnapshotBarrier, snapshotName);
        byte[] data = ZKUtil.getData(watcher, path);
        if (data.length != 0) {
          SnapshotDescriptor snapshot = getSnapshotDescription(data);
          this.listener.startSnapshot(snapshot);
        } else {
          abort("No data present in snapshot node - " + path
              + ", can't read in snaphost on zookeeper",
            new KeeperException.DataInconsistencyException());
        }
      }
    } catch (IOException e) {
      abort("Failed to read in snapshot description", e);
    }
  }

  @Override
  public void nodeDeleted(String path) {
    LOG.info("Recieved delete event:" + path);
  }

  // TODO testing around ZK - particularly node created events for children
  @Override
  public void nodeCreated(String path) {
    LOG.info("Recieved created event:" + path);
    if (path.startsWith(watcher.snapshotZNode)) {
      try {
        LOG.debug("Received snapshot info for path:" + path);
        // check to see if we are doing a new snapshot
        String node = ZKUtil.getNodeName(path);
        // if it is a simple start/end/abort then we just rewatch the node
        if (path.equals(startSnapshotBarrier)) {
          watchForNewSnapshots();
          return;
        } else if (path.equals(abortZnode)) {
          watchForAbortedSnapshots();
          return;
        }

        // otherwise we have a snapshot being created/ended
        String parent = ZKUtil.getParent(path);
        // if its the end barrier, that snapshot can be completed
        if (parent.equals(endSnapshotBarrier)) {
          LOG.debug("Recieved finsh snapshot event.");
          this.listener.finishSnapshot(node);
          return;
        } else if (parent.equals(abortZnode)) {
          this.listener.remoteSnapshotFailure(node, "Snapshot " + node + " aborted in ZK.");
          return;
        } else if (parent.equals(startSnapshotBarrier)) {
        byte[] data = ZKUtil.getData(watcher, path);
        if (data.length != 0) {
            SnapshotDescriptor snapshot = getSnapshotDescription(data);
            // TODO should be async? Right now we block until its done
            this.listener.startSnapshot(snapshot);
          }
        }else {
          abort("No data present in snapshot node - " + path
            + ", can't read in snaphost on zookeeper",
          new KeeperException.DataInconsistencyException());
      }
      } catch (KeeperException e) {
        abort("Failed to update ZK with snapshot information", e);

      } catch (IOException e) {
        abort("Failed to read in snapshot description", e);
      }
    }
  }

  private void abort(String msg, Throwable e) {
    LOG.error(msg);
    abortable.abort(msg, e);
  }

  /**
   * Join the snapshot barrier. Indicates that this server has completed its
   * part of the snapshot and is now just waiting for others to complete the
   * snapshot
   * @param snapshot snapshot that has been completed
   * @throws KeeperException if an unexpected {@link KeeperException} occurs
   */
  void joinSnapshot(SnapshotDescriptor snapshot) throws KeeperException {
    LOG.debug("Joining start barrier for snapshot (" + snapshot + ") in zk");
    String joinPath = ZKUtil.joinZNode(ZKUtil.joinZNode(startSnapshotBarrier,
      snapshot.getSnapshotNameAsString()), parent.getServerName().toString());
    ZKUtil.createAndFailSilent(watcher, joinPath);

    // watch for the complete node for this snapshot
    String completeZNode = ZKUtil.joinZNode(endSnapshotBarrier, snapshot.getSnapshotNameAsString());
    ZKUtil.watchAndCheckExists(watcher, completeZNode);
  }

  /**
   * Register with ZK this server has completed the snapshot
   * @param snapshot description of the snapshot that has been completed
   * @throws KeeperException if an unexpected {@link KeeperException} occurs
   */
  void finishSnapshot(SnapshotDescriptor snapshot) throws KeeperException {
    LOG.debug("Finishing snapshot (" + snapshot + ") in zk");
    String joinPath = ZKUtil.joinZNode(ZKUtil.joinZNode(endSnapshotBarrier,
      snapshot.getSnapshotNameAsString()), parent.getServerName().toString());
    ZKUtil.createAndFailSilent(watcher, joinPath);
  }

  /**
   * Abort the snapshot because of an internal reason (timeout, exception,etc)
   * @param snapshot snapshot that has been aborted
   * @param message reason describing why aborting
   */
  @Override
  public void snapshotFailure(SnapshotDescriptor snapshot, String message) {
    LOG.debug("Aborting snapshot (" + snapshot + ") in zk because" + message);
    String abortPath = ZKUtil.joinZNode(abortZnode, snapshot.getSnapshotNameAsString());
    try {
    // check to see if we are already aborting, and if abort the snapshot
    if (ZKUtil.checkExists(watcher, abortPath) < 0) {
      String joinPath = ZKUtil.joinZNode(abortPath, parent.getServerName().toString());
      ZKUtil.createWithParents(watcher, joinPath);
      }
    } catch (KeeperException e) {
      LOG.error("Couldn't fail snapshot properly due to ZK issue", e);
    }
  }

  public static SnapshotDescriptor getSnapshotDescription(byte[] data) throws IOException {
    SnapshotDescriptor snapshot = new SnapshotDescriptor();
    readSnapshotDescription(data, snapshot);
    return snapshot;
  }

  static void readSnapshotDescription(byte[] data, SnapshotDescriptor toRead) throws IOException {
    Preconditions.checkNotNull(data);
    Preconditions.checkArgument(data.length > 0);
    Preconditions.checkNotNull(toRead);
    // read in the data now that everything is checked
    Writables.getWritable(data, toRead);
  }
}
