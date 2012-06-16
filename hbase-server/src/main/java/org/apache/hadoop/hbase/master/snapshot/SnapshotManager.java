/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.master.snapshot;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
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

  private final HMaster master;
  private final MasterZKSnapshotController controller;

  // TODO - enable having multiple snapshots with multiple sentinels
  // snapshot monitor
  private SnapshotSentinel sentinel;

  public SnapshotManager(final HMaster master, final ZooKeeperWatcher watcher)
      throws KeeperException {

    this.controller = new MasterZKSnapshotController(watcher, this);
    this.master = master;
    this.sentinel = null;
  }

  /**
   * @return true if there is a snapshot in process, false otherwise
   */
  public boolean isInProcess() {
    return sentinel != null;
  }

  /**
   * Start managing a snapshot
   * 
   * @param hsd snapshot to monitor
   * @return SnapshotTracker for this snapshot
   * @throws IOException if there another snapshot in process
   */
  public SnapshotSentinel startSnapshot(final SnapshotDescriptor hsd)
      throws IOException {
    // only one snapshot could be monitored at a time
    // double-checked locking
    if (!isInProcess()) {
      synchronized (this) {
        if (!isInProcess()) {
          LOG.debug("Starting to monitoring snapshot: " + hsd);
          List<Pair<HRegionInfo, ServerName>> regions;

          // get the regions/servers associated with this snapshot
          try {
            regions = MetaReader.getTableRegionsAndLocations(
              master.getCatalogTracker(), hsd.getTableNameAsString());
          } catch (InterruptedException e) {
            throw new IOException(e);
          }

          // TODO - switch to using regioninfo that we are tracking
          // this is better since we let the master handle a regionserver being
          // offline at snapshot start (but leads to a potential race condition
          // that needs to be checked for if a regionserver comes online and
          // attempts to host the region that we are snapshotting)

          // setup monitoring for this snapshot
          this.sentinel = new SnapshotSentinel(hsd, master, this, Lists.transform(regions,
            new Function<Pair<HRegionInfo, ServerName>, ServerName>() {
              @Override
              public ServerName apply(Pair<HRegionInfo, ServerName> input) {
                return input.getSecond();
              }
            }));

          try {
            // kickoff the snapshot on the regionservers
            this.controller.startSnapshot(hsd);

          } catch (IOException e) {
            // make sure we abort a snapshot before bailing out
            this.sentinel.abort();
            throw e;
          } catch (KeeperException e) {
            this.sentinel.abort();
            throw new IOException(e);
          }
          return this.sentinel;
        }
      }
    }
    // XXX only true until we want to snapshot more that 1 table at a time
    LOG.error("Another snapshot is still in process: "
        + hsd.getSnapshotNameAsString());
    throw new IOException("Snapshot in process: "
        + hsd.getSnapshotNameAsString());
  }

  /**
   * @return SnapshotTracker for current snapshot
   */
  public SnapshotSentinel getCurrentSnapshotTracker() {
    return this.sentinel;
  }
  // TODO implement handling when a RS dies mid-snapshot
  // probably want to move the load to another rs or to the master
  // But for the moment, just abort the snapshot as that can lead to some tricky
  // conditions

  @Override
  public void rsAbortedSnapshot(String rsID) {
    LOG.warn("Recieved region snapshot abort for region:" + rsID);
    this.sentinel.abort();
  }

  @Override
  public void rsJoinedSnapshot(String rsID) {
    LOG.info("Regionserver: " + rsID + " is ready to take snapshot.");
    this.sentinel.serverJoinedSnapshot(rsID);
  }

  @Override
  public void rsCompletedSnapshot(String rsID) {
    LOG.info("Regionserver: " + rsID + " completed snapshot.");
    this.sentinel.serverCompletedSnapshot(rsID);
  }

  public void abort(SnapshotDescriptor hsd) throws IOException {
    // TODO add lookup into the snapshot for the given table
    // if we are managing this snapshot (when doing more than 1 snapshot)
    if (this.sentinel.getSnapshot().equals(hsd)) {
      // TODO update zk with aborting the snapshot
      // blow away the snapshot directory

      LOG.error("Failed to create snapshot, cleaning up failed snapshot: "
          + hsd);
      this.sentinel.abort();
      FSUtils.deleteDirectory(master.getMasterFileSystem().getFileSystem(),
        SnapshotDescriptor.getWorkingSnapshotDir(hsd, master.getMasterFileSystem().getRootDir()));
    }
  }

  public ZooKeeperListener getController() {
    return this.controller;
  }

  @Override
  public void allServersPreparedSnapshot() {
    try {
      LOG.debug("All servers have jong the snapshot, putting up complete node.");
      this.controller.completeSnapshot(this.sentinel.getSnapshot());
    } catch (KeeperException e) {
      // ruthless failure if we can't reach ZK
      throw new RuntimeException(e);
    }
  }
}
