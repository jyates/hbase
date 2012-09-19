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
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.CheckForSwappedStores;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.SwapStores.SwapForNonSnapshotStores;
import org.apache.hadoop.hbase.server.snapshot.errorhandling.SnapshotExceptionDispatcher;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.exception.SnapshotCreationException;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Take a snapshot on a region using the timestamp as the basis for consistency
 */
public class FlushRegionAtTimestampTask extends RegionSnapshotOperation {

  private static final Log LOG = LogFactory.getLog(FlushRegionAtTimestampTask.class);

  private final long splitPoint; // ts that decides into which store writes proceed
  private final Timer timer;
  private final MonitoredTask status;
  private Pair<Long, Long> idAndStart;
  private final CountDownLatch swappedStores = new CountDownLatch(1);

  public FlushRegionAtTimestampTask(SnapshotDescription snapshot, HRegion region,
      SnapshotExceptionDispatcher errorMonitor, RegionSnapshotOperationStatus monitor,
      long wakeFrequency, long splitPoint) {
    super(snapshot, region, errorMonitor, wakeFrequency, monitor);
    this.splitPoint = splitPoint;
    timer = new Timer("memstore-snapshot-flusher-timer", true);
    status = TaskMonitor.get().createStatus("Starting timestamp-consistent snapshot" + snapshot);
  }

  @Override
  public void prepare() throws SnapshotCreationException {
    // flush the snapshot in the future
    final CountDownLatch commitLatch = this.getAllowCommitLatch();
    TimerTask task = new TimerTask() {
      @Override
      public void run() {
        // wait for the stores to be swapped before committing
        try {
          FlushRegionAtTimestampTask.this.waitForLatch(swappedStores, "snapshot store-swap");
        } catch (HBaseSnapshotException e) {
          // ignore - this will be picked up by the main watcher
          return;
        } catch (InterruptedException e) {
          // ignore - this will be picked up by the main watcher
          return;
        } catch (Exception e) {
          // ignore - this will be picked up by the main watcher
          return;
        }
        // only run the commit step, if we don't have an error
        LOG.debug("Triggering snapshot commit - successfully swapped stores and passed swap timeout.");
        commitLatch.countDown();
      }
    };
    LOG.debug("Current split point:" + splitPoint);
    long splitDuration = splitPoint - EnvironmentEdgeManager.currentTimeMillis();
    if (splitDuration < 0) splitDuration = 0;
    LOG.debug("Scheduling snapshot commit in " + splitDuration + " ms");
    timer.schedule(task, splitDuration);

    // prepare for the snapshot
    try {
      LOG.debug("Starting snasphot on region " + this.region);
      idAndStart = this.region.startTimestampConsistentSnapshot(getSnapshot(), splitPoint,
        splitDuration, this, status);
      // notify that we have swapped the stores and are ready for commit
      swappedStores.countDown();
    } catch (IOException e) {
      throw new SnapshotCreationException(e);
    }
  }

  @Override
  public void commitSnapshot() throws SnapshotCreationException {
    // now write the in-memory state to disk for the snapshot
    // since we don't release the snapshot-write-lock, we don't get a compaction
    // and don't need to worry about on-disk state getting munged too much
    try {
      LOG.debug("Starting commit of timestamp snapshot.");
      this.region.completeTimestampConsistentSnapshot(getSnapshot(), idAndStart.getFirst(),
        idAndStart.getSecond(), this.monitor, this, status);

    } catch (ClassCastException e) {
      // happens if the stores got swapped back prematurely
      throw new SnapshotCreationException(e, this.getSnapshot());
    } catch (IOException e) {
      throw new SnapshotCreationException(e, this.getSnapshot());
    }
  }

  @Override
  public void finishSnapshot() {
    LOG.debug("Checking to see if we need to swap snapshot stores");
    CheckForSwappedStores check = new CheckForSwappedStores(this.region);
    SwapForNonSnapshotStores operator = new SwapForNonSnapshotStores(this.region);
    try {
      this.region.operateOnStores(check);
      if (check.foundSwappedStores) {
        LOG.debug("Found stores that have been swapped for snapshot stores, so swapping them back.");
        this.region.operateOnStores(operator);
      }
    } catch (IOException e) {
      LOG.error("Failed to swap back stores!", e);
      throw new RuntimeException("Failed to swap back generic stores!", e);
    }

    LOG.debug("finishing timestamp snapshot.");
    this.region.restoreRegionAfterTimestampSnapshot();
    this.status.markComplete("Completed timestamp-based snapshot (" + this.getSnapshot() + ").");
  }

  public void cleanup(Exception e) {
    LOG.debug("Cleaning up flushing region - currently a NOOP");
  }
}