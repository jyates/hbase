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
package org.apache.hadoop.hbase.regionserver.snapshot.status;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.regionserver.snapshot.RegionSnapshotPool;
import org.apache.hadoop.hbase.regionserver.snapshot.SnapshotFailureListener;
import org.apache.hadoop.hbase.regionserver.snapshot.monitor.RegionProgressMonitor;
import org.apache.hadoop.hbase.regionserver.snapshot.monitor.RunningSnapshotErrorMonitor;
import org.apache.hadoop.hbase.regionserver.snapshot.monitor.SnapshotErrorMonitor;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;

/**
 * Simple helper class to determine if a snapshot is finished or not for a set
 * of regions
 */
public class RegionSnapshotOperationStatus implements RegionProgressMonitor {

  private static final Log LOG = LogFactory.getLog(RegionSnapshotOperationStatus.class);

  private final List<RegionSnapshotStatus> results = new LinkedList<RegionSnapshotStatus>();
  private final SnapshotDescriptor desc;

  private CountDownLatch done;
  private CountDownLatch stabilized;
  private long wakeFrequency;

  // per region stability info
  private final AtomicInteger stableRegionCount = new AtomicInteger(0);
  private int totalRegions = 0;

  public RegionSnapshotOperationStatus(
      SnapshotDescriptor desc, int regionCount) {
    this.desc = desc;
    this.done = new CountDownLatch(regionCount);
    this.stabilized = new CountDownLatch(regionCount);
  }

  public boolean checkDone(RunningSnapshotErrorMonitor failureMonitor) {
    LOG.debug("Expecting " + totalRegions + " to be involved in snapshot.");
    return waitOnCondition(done, failureMonitor, "regions to complete");
  }

  public boolean waitForRegionsToStabilize(RunningSnapshotErrorMonitor failureMonitor) {
    LOG.debug("Expecting " + totalRegions + " to be involved in snapshot.");
    return waitOnCondition(stabilized, failureMonitor, "regions to stabilize");
  }

  private boolean waitOnCondition(CountDownLatch latch, SnapshotErrorMonitor failureMonitor,
      String waitingOn) {
    while (true) {
      try {
        if (this.stabilized.await(wakeFrequency, TimeUnit.MILLISECONDS)) break;
        logStatus();
        if (failureMonitor.checkForError()) {
          LOG.debug("Failure monitor found an error - not waiting for " + waitingOn);
          return false;
        }
      } catch (InterruptedException e) {
        LOG.debug("Interrupted while waiting for" + waitingOn + " for snapshot. "
            + "Ignoring and waiting some more.");
      }
    }
    return true;
  }

  private void logStatus() {
    LOG.debug("Currently have: " + (done.getCount()) + " of " + totalRegions
        + " remaining to finish snapshotting");
  }

  @Override
  public void stabilize() {
    LOG.debug("Another region has become stable.");
    this.stabilized.countDown();
  }

  @Override
  public void complete() {
    this.done.countDown();
  }
}