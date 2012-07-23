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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.snapshot.RegionSnapshotUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;
import org.apache.hadoop.hbase.snapshot.monitor.SnapshotFailureListener;

/**
 * Runnable wrapper around the the snapshot operation on a region so the
 * snapshotting can be done in parallel on the regions.
 * <p>
 * Handles running the snapshot on a single region and also that the region
 * becomes unblocked when the snapshot either completes or fails.
 */
public abstract class RegionSnapshotOperation extends SnapshotOperation {
  private static final Log LOG = LogFactory.getLog(RegionSnapshotOperation.class);

  private final Thread internal = new Thread(this);

  protected final HRegion region;
  protected final CountDownLatch joinedLatch = new CountDownLatch(1);
  private final CountDownLatch commitLatch = new CountDownLatch(1);
  protected final long wakeFrequency;
  protected final RegionSnapshotOperationStatus monitor;

  RegionSnapshotOperation(SnapshotDescriptor snapshot, HRegion region,
      SnapshotFailureListener errorListener, long wakeFrequency,
      RegionSnapshotOperationStatus monitor) {
    super(snapshot, errorListener);
    this.region = region;
    internal.setDaemon(true);
    this.wakeFrequency = wakeFrequency;
    this.monitor = monitor;
  }

  /**
   * Prepare a snapshot on the region.
   * @throws SnapshotCreationException
   */
  protected abstract void prepare() throws SnapshotCreationException;

  /**
   * Commit the snapshot - indicator from master that the snapshot can complete
   * locally.
   * @throws SnapshotCreationException
   */
  protected abstract void commit() throws SnapshotCreationException;

  /**
   * Cleanup any state that may have changed from {@link #prepare()} to
   * {@link #commit()}. This is guaranteed to run under failure situations after
   * {@link #prepare()} has been called.
   */
  protected abstract void cleanup();

  /**
   * Get a {@link Runnable} to kick off the operation's snapshot 'prepare' work.
   * This is necessary because we must have the same thread starting the region
   * operation as ending the operation, so we just pass out a very lightweight
   * thread to handle the start of the heavy-weight thread and wait for the
   * prepare phase to finish.
   * @return the runnable to launch region snapshot "prepare" work.
   */
  public Runnable getPrepareRunner() {
    // since the region running has two parts, we need a separate thread to
    // launch this thread and just wait on the finish latch
    return new Runnable() {
      @Override
      public void run() {
        internal.start();
        try {
          waitForLatch(joinedLatch, "JOIN");
        } catch (SnapshotCreationException e) {
          // ignore - this error will get picked up by the main region operation
        }
      }
    };
  }

  @Override
  public void run() {
    try {
      // get ready for snapshot
      LOG.debug("Starting snapshot on region:" + this.region);
      prepare();

      // notify that we are prepared to snapshot
      LOG.debug(this.region + " is joining snapshot.");
      this.joinedLatch.countDown();

      // wait for the indicator that we should commit
      LOG.debug(this.region + " is waiting on the commit latch.");
      waitForLatch(commitLatch, "COMMIT");
      failOnError();

      // get the files for commit
      LOG.debug(this.region + " released the commit latch, getting files.");
      commit();
      failOnError();

      // write the committed files for the snapshot
      LOG.debug(this.region + " is committing files for snapshot.");
      this.region.addRegionToSnapshot(snapshot, this);

    } catch (SnapshotCreationException e) {
      snapshotFailure("Couldn't create snapshot.", e);
    } catch (IOException e) {
      snapshotFailure("Couldn't write region hfiles to disk", e);
    } finally {
      // reset the state of the region, as necessary
      this.cleanup();
      // notify that we have completed the snapshot
      LOG.debug(this.region + " counting down finished latch.");
      this.monitor.getFinishLatch().countDown();
    }
  }

  protected CountDownLatch getCommitLatch() {
    return this.commitLatch;
  }

  /**
   * Wait for latch to count to zero, ignoring any spurious wake-ups, but waking periodically to
   * check for errors
   * @param latch latch to wait on
   * @throws SnapshotCreationException if the snapshot was failed while waiting
   */
  protected void waitForLatch(CountDownLatch latch, String latchType)
      throws SnapshotCreationException {
    RegionSnapshotUtils.waitForLatch(latch, latchType, wakeFrequency, this);
  }
}