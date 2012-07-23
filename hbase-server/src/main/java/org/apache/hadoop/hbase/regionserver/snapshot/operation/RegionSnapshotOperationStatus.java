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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.snapshot.monitor.SnapshotErrorMonitor;

/**
 * Simple helper class to determine if a snapshot is finished or not for a set
 * of regions
 */
public class RegionSnapshotOperationStatus {

  private static final Log LOG = LogFactory.getLog(RegionSnapshotOperationStatus.class);
  private long wakeFrequency;
  private final CountDownLatch done;

  // per region stability info
  protected int totalRegions = 0;

  public RegionSnapshotOperationStatus(int regionCount, long wakeFrequency) {
    this.wakeFrequency = wakeFrequency;
    this.totalRegions = regionCount;
    this.done = new CountDownLatch(regionCount);
  }

  public CountDownLatch getFinishLatch() {
    return this.done;
  }

  /**
   * @param failureMonitor monitor to check periodically for errors
   * @return <tt>true</tt> on success, <tt>false</tt> otherwise
   */
  public boolean waitUntilDone(SnapshotErrorMonitor failureMonitor) {
    LOG.debug("Expecting " + totalRegions + " to complete snapshot.");
    return waitOnCondition(done, failureMonitor, "regions to complete snapshot");
  }

  protected boolean waitOnCondition(CountDownLatch latch, SnapshotErrorMonitor failureMonitor,
      String waitingOn) {
    while (true) {
      try {
        // check to see if we had an error
        if (failureMonitor.checkForError()) {
          LOG.debug("Failure monitor found an error - not waiting for " + waitingOn);
          return false;
        }

        if (latch.await(wakeFrequency, TimeUnit.MILLISECONDS)) break;
        logStatus(latch.getCount());
      } catch (InterruptedException e) {
        LOG.debug("Interrupted while waiting for" + waitingOn + " for snapshot. "
            + "Ignoring and waiting some more.");
      }
    }
    return true;
  }

  private void logStatus(long count) {
    LOG.debug("Currently have: " + count + " of " + totalRegions
        + " remaining to finish snapshotting");
  }
}