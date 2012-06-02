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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.snapshot.status.RegionSnapshotStatus;
import org.apache.hadoop.hbase.regionserver.snapshot.status.SnapshotFailureMonitor;
import org.apache.hadoop.hbase.regionserver.snapshot.status.RegionSnapshotOperationStatus;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;

/**
 * Runnable wrapper around the the snapshot operation on a region so the
 * snapshotting can be done in parallel on the regions.
 */
class RegionSnapshotOperation extends SnapshotOperation<RegionSnapshotStatus> {
  private final HRegion region;
  private volatile boolean finished = false;
  private static final Log LOG = LogFactory.getLog(RegionSnapshotOperation.class);

  public RegionSnapshotOperation(SnapshotFailureMonitor monitor, SnapshotDescriptor snapshot,
      HRegion region) {
    super(monitor, snapshot);
    this.region = region;
  }

  /**
   * Set the status monitor.
   * <p>
   * Must be called before calling #run()
   * @param monitor progress monitor to update for the region
   */
  public void setStatusMonitor(RegionSnapshotOperationStatus monitor) {
    this.setStatus(new RegionSnapshotStatus(monitor));
  }

  @Override
  public void run() {
    try {
      region.startSnapshot(snapshot, status, this.getFailureMonitor());
      LOG.debug("Region completed snapshot, waiting to commit snapshot.");
      while (!finished) {
        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          LOG.debug("Wait for finish interrupted, done:" + finished);
        }
      }
      LOG.debug("Finishing snapshot on region:" + region);
      region.finishSnapshot();
    } catch (IOException e) {
      LOG.error("Region had an internal error and couldn't finish snapshot,", e);
      failSnapshot("Region couldn't complete taking snapshot", e);
    }
  }

  /**
   * Finish the snapshot, if it hasn't been finished already.
   * <p>
   * This can be called multiple times without worry for munging the underlying
   * region.
   */
  public synchronized void finish() {
    finished = true;
  }
}