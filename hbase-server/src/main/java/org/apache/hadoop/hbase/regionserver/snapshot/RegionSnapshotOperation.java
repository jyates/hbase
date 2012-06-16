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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.snapshot.monitor.SnapshotFailureMonitor;
import org.apache.hadoop.hbase.regionserver.snapshot.status.RegionSnapshotOperationStatus;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;

/**
 * Runnable wrapper around the the snapshot operation on a region so the
 * snapshotting can be done in parallel on the regions.
 * <p>
 * Handles running the snapshot on a single region and also that the region
 * becomes unblocked when the snapshot either completes or fails.
 */
class RegionSnapshotOperation extends SnapshotOperation {
  private static final Log LOG = LogFactory.getLog(RegionSnapshotOperation.class);
  private final HRegion region;
  private final RegionSnapshotOperationStatus status;
  private final CountDownLatch joinedLatch;
  private final CountDownLatch finishLatch;
  private final CountDownLatch completeLatch;
  private final Thread internal = new Thread(this);
  private final long wakeFrequency;

  public RegionSnapshotOperation(SnapshotDescriptor snapshot,
      HRegion region,
      SnapshotFailureMonitor errorMonitor,
 RegionSnapshotOperationStatus status, long wakeFrequency) {
    super(errorMonitor, snapshot);
    this.region = region;
    this.status = status;
    this.joinedLatch = new CountDownLatch(1);
    this.finishLatch = new CountDownLatch(1);
    this.completeLatch = new CountDownLatch(1);
    internal.setDaemon(true);
    this.wakeFrequency = wakeFrequency;
  }

  /**
   * Get a {@link Runnable} to kick off the operation's snapshot 'prepare' work.
   * This is necessary because we must have the same thread starting the region
   * operation as ending the operation, so we just pass out a very lightweight
   * thread to handle the start of the heavyweight thread and wait for the
   * prepare phase to finish.
   * @return the runnable around the operation's "prepare" work.
   */
  public Runnable runner(){
    // since the region running has two parts, we need a separate thread to
    // launch this thread and just wait on the finish latch
    return new Runnable(){
      @Override
      public void run() {
        internal.start();
        try {
          waitForLatch(joinedLatch);
        } catch (SnapshotCreationException e) {
          // ignore - this error will get picked up by the main region operation
        }
      }
    };
  }

  @Override
  public void run() {
    // XXX its kind of frustrating that we need to go through all this pain for
    // synchronization on a region - only use the same thread because of the
    // internal locking requires the same thread to unlock as locked the region.
    LOG.debug("Starting snapshot on region.");
    try {
      region.startSnapshot(snapshot, status, this.errorMonitor);
      LOG.debug("Region completed snapshot, waiting to commit snapshot.");
      this.joinedLatch.countDown();

      // wait until the finish latch has been triggered
      waitForLatch(finishLatch);

      // finish the snapshot in the finally block to ensure the region is
      // available after a snapshot finishes/completes
      LOG.debug("Finishing snapshot on region:" + region);
    } catch (Exception e) {
      LOG.error("Region had an internal error and couldn't finish snapshot,", e);
      failSnapshot("Region couldn't complete taking snapshot", e);
    } finally {
      LOG.debug("Cleaning up snapshot for region");
      region.finishSnapshot();
    }
    this.completeLatch.countDown();
  }

  /**
   * Finish the snapshot, if it hasn't been finished already.
   * <p>
   * This can be called multiple times without worry for munging the underlying
   * region.
   */
  public void finish() {
    this.finishLatch.countDown();
    LOG.debug("Counted down latch.(" + this.finishLatch.getCount() + ")");
    try {
      waitForLatch(completeLatch);
    } catch (SnapshotCreationException e) {
      // ignore - we are done and just finish
    }
  }

  /**
   * Wait for latch to count to zero, ignoring any spurious wake-ups, but waking
   * periodically to check for errors
   * @param latch latch to wait on
   * @throws SnapshotCreationException if the snapshot was failed while waiting
   */
  private void waitForLatch(CountDownLatch latch) throws SnapshotCreationException {
    do {
      // first check for error, and if none is found then wait
      if (checkForError()) {
        throw new SnapshotCreationException("Found an error while waiting for latch.");
      }
      try {
        LOG.debug("Waiting for snapshot progress latch.");
        latch.await(wakeFrequency, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        LOG.debug("Wait for latch interrupted, done:" + (latch.getCount() == 0));
      }
    } while (latch.getCount() > 0);
  }

}