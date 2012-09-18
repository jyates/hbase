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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionCheckable;
import org.apache.hadoop.hbase.server.snapshot.error.SnapshotErrorListener;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;
import org.apache.hadoop.hbase.util.Threads;

/**
 * Simple helper class to determine if a snapshot is finished or not for a set of regions
 */
public class RegionSnapshotOperationStatus {

  private static final Log LOG = LogFactory.getLog(RegionSnapshotOperationStatus.class);
  private long wakeFrequency;
  private final CountDownLatch done;

  // per region stability info
  protected int totalRegions = 0;

  /**
   * Monitor the status of the operation.
   * @param regionCount number of regions to complete the operation before the operation is ready.
   *          Wait on all regions via {@link #waitUntilDone(SnapshotErrorListener)}.
   * @param wakeFrequency frequency to check for errors while waiting for regions to prepare
   */
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
  public boolean waitUntilDone(SnapshotErrorListener failureMonitor) {
    LOG.debug("Expecting " + totalRegions + " to complete snapshot.");
    return waitOnCondition(done, failureMonitor, "regions to complete snapshot");
  }

  protected boolean waitOnCondition(CountDownLatch latch,
      ExceptionCheckable<HBaseSnapshotException> failureMonitor, String info) {
    try {
      Threads.waitForLatch(latch, failureMonitor, wakeFrequency, info);
    } catch (HBaseSnapshotException e) {
      LOG.warn("Error found while :" + info, e);
      return false;
    } catch (InterruptedException e) {
      LOG.error("Interrupted while waiting for :" + info);
      return false;
    }
    // if there was an error, then we can't claim success
    return failureMonitor.checkForError() ? false : true;
  }
}