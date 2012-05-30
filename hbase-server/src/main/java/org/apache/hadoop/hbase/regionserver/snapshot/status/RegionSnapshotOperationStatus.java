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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.regionserver.snapshot.RegionSnapshotPool;
import org.apache.hadoop.hbase.regionserver.snapshot.SnapshotFailureListener;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;

/**
 * Simple helper class to determine if a snapshot is finished or not for a set
 * of regions
 */
// TODO does this need to implement HasThread?
public class RegionSnapshotOperationStatus extends SnapshotStatus {

  private static final Log LOG = LogFactory.getLog(RegionSnapshotOperationStatus.class);

  private final SnapshotFailureMonitor failureMonitor;
  private final List<RegionSnapshotStatus> results = new LinkedList<RegionSnapshotStatus>();
  private final SnapshotDescriptor desc;

  private volatile boolean done = false;

  // per region stability info
  private final AtomicInteger stableRegionCount = new AtomicInteger(0);
  private int totalRegions = 0;
  private final long wakeFrequency;

  public RegionSnapshotOperationStatus(SnapshotFailureMonitor failureMonitor,
      SnapshotDescriptor desc, int regionCount, long wakeFrequency) {
    this.failureMonitor = failureMonitor;
    this.desc = desc;
    this.wakeFrequency = wakeFrequency;
  }

  @Override
  public boolean checkDone() {
    LOG.debug("Expecting " + totalRegions + " to be involved in snapshot.");
    for (int i = 0; i < results.size(); i++) {
      try {
        if (results.get(i).isDone()) {
          results.get(i).getResult();
          results.remove(i--);
        }
      }
      // if there is any kind of failure, everyone fails
      catch (ExecutionException e) {
        failureMonitor.localSnapshotFailure(desc, "Region had an exception while snapshotting:"
            + e.getMessage());
        // cancel all the remaing tasks because we failed the snapshot
        cancelOperations();
        break;
      } catch (InterruptedException e) {
        failureMonitor.localSnapshotFailure(desc, "Region had an exception while snapshotting:"
            + e.getMessage());
        cancelOperations();
        break;
      } catch (CancellationException e) {
        // ignore - cancellation occurs if we cancelled it already
        break;
      } catch (SnapshotCreationException e) {
        cancelOperations();
        break;
      }
      logStatus();
    }

    if (this.results.size() == 0) {
      LOG.debug("All regions done snapshotting");
      return true;
    }
    LOG.debug("Stil have some regions left to finish snapshotting.");
    return false;
  }

  @Override
  public String getStatus() {
    return "Currently have: " + (results.size()) + " of " + totalRegions
        + " remaining to finish snapshotting";
  }

  private void logStatus() {
    LOG.debug(getStatus());
  }

  private void cancelOperations() {
    for (SnapshotStatus op : results) {
      op.cancel();
    }
  }

  /**
   * Check to see if all the regions have reached a 'stable' state where no
   * new writes are incoming
   * @return <tt>true</tt> if all the regions are stable
   * @throws SnapshotCreationException if the snapshot has failed for any
   *           reason (timeout, region failure, external snapshot failure)
   */
  public boolean allRegionsStable() throws SnapshotCreationException {
    return !(stableRegionCount.intValue() < totalRegions);
  }

  public void regionBecameStable() {
    LOG.debug("Another region has become stable.");
    stableRegionCount.incrementAndGet();

  }

  /**
   * Add a subtask to monitor for completion
   * @param regionWork
   */
  public void addStatus(RegionSnapshotStatus regionWork) {
    this.results.add(regionWork);
    totalRegions++;
  }
}