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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.snapshot.monitor.DistributedSnapshotMonitor;
import org.apache.hadoop.hbase.master.snapshot.monitor.MasterSnapshotStatusListener;
import org.apache.hadoop.hbase.snapshot.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;
import org.apache.hadoop.hbase.snapshot.SnapshotInProgressException;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
 * This class monitors the whole process of snapshots via ZooKeeper. There is
 * only one SnapshotMonitor for the master.
 * <p>
 * Start monitoring a snapshot by calling method monitor() before the snapshot
 * is started across the cluster via ZooKeeper. SnapshotMonitor would stop
 * monitoring this snapshot only if it is finished or aborted.
 * <p>
 * Note: There could be only one snapshot being processed and monitored at a
 * time over the cluster. Start monitoring a snapshot only when the previous one
 * reaches an end status.
 */
@InterfaceAudience.Private
public class SnapshotManager implements MasterSnapshotStatusListener {
  private static final Log LOG = LogFactory.getLog(SnapshotManager.class);

  private final MasterServices master;
  private final MasterZKSnapshotController controller;

  // TODO - enable having multiple snapshots with multiple sentinels
  private SnapshotSentinel sentinel;
  private DistributedSnapshotMonitor monitor;

  public SnapshotManager(final MasterServices master, final ZooKeeperWatcher watcher)
      throws KeeperException {

    this.controller = new MasterZKSnapshotController(watcher, this);
    this.master = master;
    this.sentinel = null;
    this.monitor = null;
  }

  /**
   * @return true if there is a snapshot in process, false otherwise
   */
  public boolean isInProcess() {
    return sentinel != null;
  }

  /**
   * Start managing a snapshot
   * @param hsd snapshot to start managing
   * @return Snapshot sentinel for this snapshot to launch a snapshot via
   *         {@link SnapshotSentinel#run()}
   * @throws HBaseSnapshotException if there another snapshot in process
   */
  public synchronized SnapshotSentinel startManagingOnlineSnapshot(final SnapshotDescriptor hsd)
      throws HBaseSnapshotException {
    // only one snapshot could be monitored at a time
    // double-checked locking
    if (isInProcess()) throw new SnapshotInProgressException(
        "Snapshot already being run on the server!", this.sentinel.getSnapshot());
    LOG.debug("Starting to monitor snapshot: " + hsd);
    List<Pair<HRegionInfo, ServerName>> regions;

    // get the regions/servers associated with this snapshot
    try {
      regions = MetaReader.getTableRegionsAndLocations(master.getCatalogTracker(),
        hsd.getTableNameAsString());
    } catch (InterruptedException e) {
      throw new HBaseSnapshotException(e);
    } catch (IOException e) {
      throw new HBaseSnapshotException(e);
    }

    // setup monitoring for this snapshot
    try {
    OnlineSnapshotSentinel sentinel = new OnlineSnapshotSentinel(hsd, master, this,
        Lists.transform(regions, new Function<Pair<HRegionInfo, ServerName>, ServerName>() {
          @Override
          public ServerName apply(Pair<HRegionInfo, ServerName> input) {
            return input.getSecond();
          }
        }));
    // setup the sentinel and monitor
    this.monitor = sentinel;
    this.sentinel = sentinel;


      // kickoff the snapshot on the regionservers
      this.controller.startSnapshot(hsd);

    } catch (IOException e) {
      // make sure we abort a snapshot before bailing out
      this.sentinel.snapshotFailure("Failed to start snapshot in manager", e);
      resetSnapshotMonitors();
      throw new HBaseSnapshotException(e);
    } catch (KeeperException e) {
      this.sentinel.snapshotFailure("Failed to start snapshot in manager", e);
      resetSnapshotMonitors();
      throw new HBaseSnapshotException(e);
    }
    return this.sentinel;
  }

  /**
   * Create a snapshot of an offline table
   * @param hsd descriptor of the snapshot to take
   * @param isSnapshotDone global boolean as to a snapshot being completed or not
   * @param executorService thread pool to run the underlying operation
   * @param master provide basic services from the master
   * @param server server where the snapshot is being run
   * @return {@link SnapshotSentinel} that will run a snapshot on an offline table on the master
   * @throws IOException
   */
  public SnapshotSentinel startManagingOfflineSnapshot(SnapshotDescriptor hsd,
      AtomicBoolean isSnapshotDone, ExecutorService executorService, MasterServices master,
      Server server) throws IOException {
    if (!isInProcess()) {
      synchronized (this) {
        if (!isInProcess()) {
          LOG.debug("Starting to monitoring snapshot: " + hsd);
          try {
            this.sentinel = new OfflineSnapshotSentinel(hsd, master, server, isSnapshotDone,
                executorService);
            return this.sentinel;
          } catch (IOException e) {
            this.sentinel = null;
            throw e;
          }
        }
      }
    }

    LOG.error("Another snapshot is still in process: " + hsd.getSnapshotNameAsString());
    throw new IOException("Snapshot in process: " + hsd.getSnapshotNameAsString());
  }

  /**
   * @return SnapshotTracker for current snapshot
   */
  public SnapshotSentinel getCurrentSnapshotTracker() {
    return this.sentinel;
  }

  @Override
  public void rsAbortedSnapshot(String rsID) {
    String message = "Recieved snapshot abort from region:" + rsID;
    LOG.debug(message);
    this.sentinel.snapshotFailure(message, null);
  }

  @Override
  public void rsJoinedSnapshot(String rsID) {
    LOG.info("Regionserver: " + rsID + " is ready to take snapshot.");
    if (this.monitor != null) monitor.serverJoinedSnapshot(rsID);
  }

  @Override
  public void rsCompletedSnapshot(String rsID) {
    LOG.info("Regionserver: " + rsID + " completed snapshot.");
    if (this.monitor != null) monitor.serverCompletedSnapshot(rsID);
  }

  @Override
  public void allServersPreparedSnapshot() {
    try {
      LOG.debug("All servers have joined the snapshot, putting up complete node.");
      this.controller.completeSnapshot(this.sentinel.getSnapshot());
    } catch (KeeperException e) {
      // ruthless failure if we can't reach ZK
      throw new RuntimeException(e);
    }
  }

  /**
   * Abort the given snapshot, if we know about it
   * @param hsd {@link SnapshotDescriptor} of the snapshot to abort
   * @throws IOException if the directory couldn't be cleaned up
   */
  public void abort(SnapshotDescriptor hsd) throws IOException {
    if (this.sentinel == null) return;
    // blow away the snapshot directory
    String msg = "Failed to create snapshot, cleaning up failed snapshot: " + hsd;
    this.sentinel.snapshotFailure(hsd, msg);
    FSUtils.deleteDirectory(master.getMasterFileSystem().getFileSystem(),
      SnapshotDescriptor.getWorkingSnapshotDir(hsd, FSUtils.getRootDir(master.getConfiguration())));
  }

  /**
   * Reset the manager to allow another snapshot to precede
   * @param snapshotDir final path of the snapshot
   * @param workingDir directory where the in progress snapshot was built
   * @param fs {@link FileSystem} where the snapshot was built
   * @throws IOException if the snapshot couldn't be correctly reset (e.g. internal ZooKeeper
   *           error).
   */
  public void completeSnapshot(Path snapshotDir, Path workingDir, FileSystem fs) throws IOException {
    if (this.sentinel == null) return;
    LOG.debug("Sentinel is done, just moving the snapshot from " + workingDir + " to "
        + snapshotDir);
    if (!fs.rename(workingDir, snapshotDir)) {
      throw new IOException("Failed to copy working directory to completed directory.");
    }

  }

  /**
   * Cleanup the leftover state from a snapshot, regardless of the snapshot failing or being
   * successful
   * @param workingDir working directory for the snapshot
   * @param fs filesystem where the snapshot is stored
   * @throws IOException on unexpected failure to delete the fs or zk state
   */
  public void cleanupSnapshot(Path workingDir, FileSystem fs) throws IOException {
    // cleanup the working directory
    if (!fs.delete(workingDir, true)) {
      LOG.warn("Failed to delete snapshot working directory:" + workingDir);
    }
    try {
      if (this.sentinel != null) {
        LOG.debug("Reseting zookeeper snapshot state");
        controller.resetZooKeeper(this.sentinel.getSnapshot());
      }
    } catch (KeeperException e) {
      throw new IOException(e);
    } finally {
      LOG.debug("Reseting sentinel and zk monitor");
      resetSnapshotMonitors();
    }
  }

  /**
   * Reset the snapshot monitor and senitinel
   */
  private void resetSnapshotMonitors() {
    this.monitor = null;
    this.sentinel = null;
  }
}