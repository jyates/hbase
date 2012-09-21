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
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;

/**
 * Progress monitor for a region that is doing a globall consistent snapshot
 */
public class GlobalRegionSnapshotProgressMonitor extends RegionSnapshotOperationStatus {
  private static final Log LOG = LogFactory.getLog(GlobalRegionSnapshotProgressMonitor.class);
  private CountDownLatch stabilized;

  public GlobalRegionSnapshotProgressMonitor(int regionCount, long wakeFrequency) {
    super(regionCount, wakeFrequency);
    this.stabilized = new CountDownLatch(regionCount);
  }

  /**
   * Wait for the all involved regions to become 'stable' (blocking writes).
   * @param failureMonitor monitor to check for errors to the operation
   * @return <tt>true</tt> on success, <tt>false</tt> if an error was detected while waiting.
   */
  public boolean waitForRegionsToStabilize(
ExceptionCheckable<HBaseSnapshotException> failureMonitor) {
    LOG.debug("Expecting " + totalRegions + " regions to eventually stabilize in snapshot.");
    return waitOnCondition(stabilized, failureMonitor, "regions to stabilize");
  }

  public void stabilize() {
    LOG.debug("Another region has become stable.");
    this.stabilized.countDown();
  }
}