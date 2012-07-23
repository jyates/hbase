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
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;
import org.apache.hadoop.hbase.snapshot.monitor.SnapshotFailureListener;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Take a snapshot on a region using the timestamp as the basis for consistency
 */
public class TimestampRegionSnapshotOperation extends RegionSnapshotOperation {

  private static final Log LOG = LogFactory.getLog(TimestampRegionSnapshotOperation.class);

  private final long splitPoint;
  private final Timer timer;
  private final MonitoredTask status;
  private Pair<Long, Long> idAndStart;
  private final CountDownLatch swappedStores = new CountDownLatch(1);

  public TimestampRegionSnapshotOperation(SnapshotDescriptor snapshot, HRegion region,
      SnapshotFailureListener errorMonitor, RegionSnapshotOperationStatus monitor,
      long wakeFrequency, long splitPoint) {
    super(snapshot, region, errorMonitor, wakeFrequency, monitor);
    this.splitPoint = splitPoint;
    timer = new Timer("memstore-snapshot-flusher", true);
    status = TaskMonitor.get().createStatus("Starting timestamp-consistent snapshot" + snapshot);
  }

  @Override
  public void prepare() throws SnapshotCreationException {
    // flush the snapshot in the future
    final CountDownLatch commitLatch = this.getCommitLatch();
    TimerTask task = new TimerTask() {
      @Override
      public void run() {
        // wait for the stores to be swapped before committing
        try {
          // TODO this doesn't need to actually wait, as long as we check for the right store in
          // commit.
          TimestampRegionSnapshotOperation.this.waitForLatch(swappedStores, "Store-swap");
        } catch (SnapshotCreationException e) {
          // ignore - this will be picked up by the main watcher
        }
        commitLatch.countDown();
        LOG.debug("Triggering snapshot commit");
      }
    };
    long splitDuration = splitPoint - EnvironmentEdgeManager.currentTimeMillis();
    if (splitDuration < 0) splitDuration = 0;
    LOG.debug("Scheduling snapshot commit in " + splitDuration + " ms");
    timer.schedule(task, splitDuration);

    // prepare for the snapshot
    try {
      idAndStart = this.region.startTimestampConsistentSnapshot(snapshot, splitPoint,
        splitDuration, this, status);
      // notify that we have swapped the stores and are ready for commit
      swappedStores.countDown();
    } catch (IOException e) {
      throw new SnapshotCreationException(e);
    }
  }

  @Override
  public void commit() throws SnapshotCreationException {
    // now write the in-memory state to disk for the snapshot
    // since we don't release the snapshot-write-lock, we don't get a compaction
    // and don't need to worry about on-disk state getting munged too much
    try {
      this.region.completeTimestampConsistentSnapshot(snapshot, idAndStart.getFirst(),
        idAndStart.getSecond(), this.monitor, this, status);
    } catch (IOException e) {
      throw new SnapshotCreationException(e);
    }
  }

  @Override
  public void cleanup() {
    this.region.cleanupTimestampSnapshot();
    this.status.markComplete("Completed timestamp-based snapshot ("
        + this.snapshot + ").");
  }
}
