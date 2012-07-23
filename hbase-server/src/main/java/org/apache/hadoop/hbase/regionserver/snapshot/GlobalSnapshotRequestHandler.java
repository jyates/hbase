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
package org.apache.hadoop.hbase.regionserver.snapshot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.snapshot.operation.GlobalRegionSnapshotOperation;
import org.apache.hadoop.hbase.regionserver.snapshot.operation.GlobalRegionSnapshotProgressMonitor;
import org.apache.hadoop.hbase.regionserver.snapshot.operation.RegionSnapshotOperation;
import org.apache.hadoop.hbase.regionserver.snapshot.operation.TableInfoCopyOperation;
import org.apache.hadoop.hbase.regionserver.snapshot.operation.WALReferenceOperation;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;
import org.apache.hadoop.hbase.snapshot.monitor.RunningSnapshotFailureMonitor;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * Snapshot request handler that runs a stop-the-world snapshot on a single
 * table.
 */
public class GlobalSnapshotRequestHandler extends
    SnapshotRequestHandler<GlobalRegionSnapshotProgressMonitor> {
  private static final Log LOG = LogFactory.getLog(GlobalSnapshotRequestHandler.class);

  private final HLog log;

  public GlobalSnapshotRequestHandler(SnapshotDescriptor snapshot, List<HRegion> regions, HLog log,
      RegionServerServices rss, RunningSnapshotFailureMonitor errorMonitor, long wakeFreq,
      ExecutorService pool) {
    super(snapshot, regions, rss, errorMonitor, wakeFreq, pool,
        new GlobalRegionSnapshotProgressMonitor(regions.size(), wakeFreq));
    this.log = log;
  }

  @Override
  public boolean startSnapshot() {
    long now = EnvironmentEdgeManager.currentTimeMillis();
    // Start snapshotting all the involved regions
    try {
      // 1. create an operation for each region
      this.ops = new ArrayList<RegionSnapshotOperation>(regions.size());
      for (HRegion region : regions) {
        ops.add(new GlobalRegionSnapshotOperation(snapshot, region, errorMonitor, progressMonitor,
            wakeFrequency));
      }

      // 2. submit those operations to the region snapshot runner
      for (RegionSnapshotOperation op : ops)
        submitTask(op.getPrepareRunner());

      // 2.1 do the copy table async at the same time
      if (errorMonitor.checkForError()) return false;
      submitTask(new TableInfoCopyOperation(errorMonitor, snapshot, this.rss.getFileSystem(),
          FSUtils.getRootDir(rss.getConfiguration())));

      // wait for all the regions to become stable (no more writes) so we can
      // put a reliable snapshot point in the WAL
      if (!progressMonitor.waitForRegionsToStabilize(errorMonitor)) return false;

      // 3. append an edit in the WAL marking our position
      // append a marker for the snapshot to the walog
      // ensures the walog is synched to this point
      WALEdit edit = HLog.getSnapshotWALEdit();
      HRegionInfo regionInfo = new ServerRegionInfo(snapshot.getTableName());
      this.log.append(regionInfo, snapshot.getTableName(), edit, now, this.regions.get(0)
          .getTableDesc());

      // 3.1 asynchronously add reference for the WALs
      submitTask(new WALReferenceOperation(snapshot, errorMonitor, this.log.getDir(),
          rss.getConfiguration(), rss.getFileSystem(), rss.getServerName().toString()));

      // 4. Wait for the regions and the wal to complete or an error
      // wait for the regions to complete their snapshotting
      waitForOutstandingTasks();
    } catch (RejectedExecutionException e) {
      return failAndReturn("Failing snapshot because we couldn't run a part of the snapshot", e);
    } catch (SnapshotCreationException e) {
      return failAndReturn("Failing snapshot because got creation exception", e);
    } catch (IOException e) {
      return failAndReturn("Failing snapshot because got general IOException", e);
    }
    return true;
  }

  @Override
  public boolean commitSnapshot() {
    // Release all the locks taken on the involved regions
    if (ops == null || ops.size() == 0) {
      LOG.debug("No regions to release from the snapshot because we didn't get a chance to create them.");
      return true;
    }
    LOG.debug("Releasing local barrier for snapshot.");
    for (RegionSnapshotOperation op : ops) {
      ((GlobalRegionSnapshotOperation) op).commitSnapshot();
    }
    return waitAndReturnDone();
  }

  /**
   * Simple helper class that allows server-level meta edits to the WAL
   * <p>
   * These edits are NOT intended to be replayed on server failure or by the standard WAL player
   */
  private static class ServerRegionInfo extends HRegionInfo {
    private static final String MOCK_INFO_NAME_PREFIX = "_$NOT_A_REGION$_";

    public ServerRegionInfo(byte[] tablename) {
      super(tablename);
    }

    @Override
    public synchronized String getEncodedName() {
      return MOCK_INFO_NAME_PREFIX + super.getEncodedName();
    }

    @Override
    public boolean isMetaRegion() {
      return false;
    }
  }
}