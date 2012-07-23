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
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.snapshot.operation.RegionSnapshotOperation;
import org.apache.hadoop.hbase.regionserver.snapshot.operation.RegionSnapshotOperationStatus;
import org.apache.hadoop.hbase.regionserver.snapshot.operation.TableInfoCopyOperation;
import org.apache.hadoop.hbase.regionserver.snapshot.operation.TimestampRegionSnapshotOperation;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;
import org.apache.hadoop.hbase.snapshot.monitor.RunningSnapshotFailureMonitor;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * Handle a snapshot request for a timestamp-consistent snapshot
 */
public class TimestampSnapshotRequestHandler extends
    SnapshotRequestHandler<RegionSnapshotOperationStatus> {
  private static final Log LOG = LogFactory.getLog(TimestampSnapshotRequestHandler.class);

  private final long splitPoint;

  public TimestampSnapshotRequestHandler(SnapshotDescriptor snapshot, List<HRegion> regions,
      RegionServerServices rss, RunningSnapshotFailureMonitor monitor, long wakeFreq,
      ExecutorService pool, long splitPoint) {
    super(snapshot, regions, rss, monitor, wakeFreq, pool, new RegionSnapshotOperationStatus(
        regions.size(), wakeFreq));
    long time = splitPoint - EnvironmentEdgeManager.currentTimeMillis();
    if (time <= 0) {
      LOG.debug("Split duration <= 0, flushing snapshot immediately.");
      time = 0;
    }
    this.splitPoint = time;
  }

  @Override
  public boolean startSnapshot() {
    try {
      // 1. create an operation for each region
      this.ops = new ArrayList<RegionSnapshotOperation>(regions.size());
      for (HRegion region : regions) {
        ops.add(new TimestampRegionSnapshotOperation(snapshot, region, errorMonitor,
            progressMonitor, wakeFrequency, splitPoint));
      }

      // 2. submit those operations to the region snapshot runner
      for (RegionSnapshotOperation op : ops)
        submitTask(op.getPrepareRunner());

      // 3. do the tableinfo copy async
      if (errorMonitor.checkForError()) return false;
      submitTask(new TableInfoCopyOperation(errorMonitor, snapshot, this.rss.getFileSystem(),
          FSUtils.getRootDir(this.rss.getConfiguration())));

      // 4. Wait for the regions to complete their snapshotting or an error
      waitForOutstandingTasks();
    } catch (IOException e) {
      return failAndReturn("Failed to get the root directory for the filesystem from conf", e);
    } catch (RejectedExecutionException e) {
      return failAndReturn("Failing snapshot because we couldn't run a part of the snapshot", e);
    }
    return true;
  }

  @Override
  public boolean commitSnapshot() {
    // wait for the snapshot to complete on all the regions
    return this.waitAndReturnDone();
  }

}
