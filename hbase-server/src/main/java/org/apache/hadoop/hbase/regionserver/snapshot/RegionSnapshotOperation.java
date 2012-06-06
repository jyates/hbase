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
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.snapshot.status.RegionSnapshotStatus;
import org.apache.hadoop.hbase.regionserver.snapshot.status.SnapshotFailureMonitor;
import org.apache.hadoop.hbase.regionserver.snapshot.status.RegionSnapshotOperationStatus;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;

/**
 * Runnable wrapper around the the snapshot operation on a region so the
 * snapshoting can be done in parallel on the regions.
 */
class RegionSnapshotOperation extends SnapshotOperation<RegionSnapshotStatus> {
  private static final Log LOG = LogFactory.getLog(RegionSnapshotOperation.class);
  private final HRegion region;
  private final RegionSnapshotOperationStatus status;
  private CountDownLatch finishLatch;
  private CountDownLatch completeLatch;

  public RegionSnapshotOperation(SnapshotDescriptor snapshot,
      HRegion region, SnapshotFailureMonitor monitor, RegionSnapshotOperationStatus status) {
    super(monitor, snapshot, status);
    this.region = region;
    this.status = status;
    this.finishLatch = new CountDownLatch(1);
    this.completeLatch = new CountDownLatch(1);
  }

  @Override
  public void run() {
    // XXX its kind of frustrating that we need to go through all this pain for
    // synchronization on a region - only use the same thread because of the
    // internal locking requires the same thread to unlock as locked the region.
    try {
      region.startSnapshot(snapshot, status, this.getFailureMonitor());
      LOG.debug("Region completed snapshot, waiting to commit snapshot.");

      while (this.finishLatch.getCount() != 0) {
        try {
          // Thread.sleep(50);
          LOG.debug("Waiting to finish the snapshot.");
          this.finishLatch.await();
          LOG.debug("Latch at " + this.finishLatch.getCount() + "!");
        } catch (InterruptedException e) {
          LOG.debug("Wait for finish interrupted, done:" + (this.finishLatch.getCount() == 0));
        }
      }
      LOG.debug("Finishing snapshot on region:" + region);
      region.finishSnapshot();
    } catch (IOException e) {
      LOG.error("Region had an internal error and couldn't finish snapshot,", e);
      failSnapshot("Region couldn't complete taking snapshot", e);
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
      LOG.debug("Waiting for region to complete snapshot...");
        completeLatch.await();
      } catch (InterruptedException e) {
        LOG.debug("Wait for complete interrupted, done:" + completeLatch.getCount());
      }
  }

}