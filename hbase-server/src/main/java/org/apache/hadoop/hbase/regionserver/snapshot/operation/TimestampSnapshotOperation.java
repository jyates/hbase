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
package org.apache.hadoop.hbase.regionserver.snapshot.operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.server.commit.ThreePhaseCommit;
import org.apache.hadoop.hbase.server.commit.distributed.DistributedCommitException;
import org.apache.hadoop.hbase.server.commit.distributed.DistributedThreePhaseCommitErrorDispatcher;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionVisitor;
import org.apache.hadoop.hbase.server.snapshot.error.SnapshotErrorMonitorFactory;
import org.apache.hadoop.hbase.server.snapshot.errorhandling.SnapshotExceptionDispatcher;
import org.apache.hadoop.hbase.server.snapshot.task.TableInfoCopyTask;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * Take a timestamp-consistent snapshot for a set of regions of a table on a regionserver
 */
public class TimestampSnapshotOperation extends SnapshotOperation {

  private static final Log LOG = LogFactory.getLog(TimestampSnapshotOperation.class);
  private final long splitPoint;
  private final RegionSnapshotOperationStatus progressMonitor;

  public TimestampSnapshotOperation(DistributedThreePhaseCommitErrorDispatcher errorListener,
      long wakeFrequency, long timeout,
      List<HRegion> regions, SnapshotDescription snapshot, Configuration conf,
      SnapshotTaskManager taskManager, SnapshotErrorMonitorFactory monitorFactory, FileSystem fs) {
    super(errorListener, wakeFrequency, timeout, regions, snapshot, conf,
        taskManager, monitorFactory, fs);
    // setup write partitioning information
    long time = snapshot.getCreationTime() - EnvironmentEdgeManager.currentTimeMillis();
    if (time <= 0) {
      LOG.debug("Split duration <= 0, flushing snapshot immediately.");
      time = 0;
    }
    this.splitPoint = time;

    // create a progress monitor to keep track of the each region's snapshot progress
    this.progressMonitor = new RegionSnapshotOperationStatus(regions.size(), wakeFrequency);
  }

  @Override
  public void prepare() throws DistributedCommitException {
    try {
      // 1. create a flush operation for each region
      this.ops = new ArrayList<RegionSnapshotOperation>(regions.size());
      for (HRegion region : regions) {
        ops.add(new FlushRegionAtTimestampTask(snapshot, region, snapshotErrorListener,
            progressMonitor, wakeFrequency, splitPoint));
      }

      // 2. submit those operations to the region snapshot runner
      for (RegionSnapshotOperation op : ops)
        taskManager.submitTask(op);

      // 3. do the tableinfo copy async
      this.snapshotErrorListener.failOnError();
      submitTableInfoCopy();

      // 4. Wait for the regions to complete their snapshotting or an error
      this.taskManager.waitForOutstandingTasks(this.getErrorCheckable());
    } catch (IOException e) {
      throw wrapExceptionForSnapshot(e);
    } finally {
      LOG.debug("Done preparing timestamp request.");
    }
  }

  @Override
  public void commit() throws DistributedCommitException {
    // wait for the snapshot to complete on all the regions
    LOG.debug("Waiting for operations to complete.");
    if (progressMonitor.waitUntilDone(this.snapshotErrorListener)) {
      throw wrapExceptionForSnapshot(new HBaseSnapshotException(
          "Found an error while waiting for snapshot to complete, quiting!"));
    }
  }
}