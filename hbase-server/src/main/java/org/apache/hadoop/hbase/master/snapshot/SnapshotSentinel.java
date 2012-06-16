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
package org.apache.hadoop.hbase.master.snapshot;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * Tracking the global status of a snapshot over its lifetime. Each snapshot is
 * associated with a SnapshotSentinel. It is created at the start of snapshot
 * and is destroyed when the snapshot reaches one of the end status:
 * <code>GlobalSnapshotStatus.ALL_RS_FINISHED<code> or
 * <code>GlobalSnapshotStatus.ABORTED<code>.
 */
@InterfaceAudience.Private
public class SnapshotSentinel {
  /**
   * Status of snapshot over the cluster. The end status of snapshot is
   * <code>ALL_RS_FINISHED</code> or <code>ABORTED</code>.
   */
  public static enum GlobalSnapshotStatus {
    INIT, // snapshot is just started over the cluster
    RS_PREPARING_SNAPSHOT, // some RS are still preparing the snapshot
    RS_COMMITTING_SNAPSHOT, // RS are committing the snapshot
    ALL_RS_FINISHED, // all RS have finished committing the snapshot
    ABORTING; // abort the snapshot over the cluster
  }

  /** By default, wait 30 seconds for a snapshot to complete */
  private static final long DEFAULT_MAX_WAIT_TIME = 30000;
  /** Max amount of time to wait for a snapshot to complete (milliseconds) */
  private static final String MASTER_SNAPSHOT_MAX_WAIT_TIME = "hbase.master.snapshot.maxTime";

  private static final Log LOG = LogFactory.getLog(SnapshotSentinel.class);

  /** By default, check to see if the snapshot is complete every 50ms */
  private static final int DEFAULT_MAX_WAIT_FREQUENCY = 500;

  private static final String MAX_WAIT_FREQUENCY = "hbase.master.snapshot.wakeFrequency";

  private final long maxWait;
  private final long wakeFrequency;
  private final SnapshotDescriptor hsd;

  // current status of snapshot
  private volatile GlobalSnapshotStatus status;


  /**
   * active servers when the snapshot is started. The table for snapshot is
   * served by these servers so snapshot is finished only if finished on all
   * these region servers.
   */
  private List<ServerName> waitToPrepare;

  /**
   * The list of servers waiting to finish the snapshot.Servers that have
   * finished preparing are moved to waitToFinish.
   */
  private List<String> waitToFinish = new LinkedList<String>();

  private final MasterSnapshotStatusListener progressListener;
  private CountDownLatch prepareLatch;
  private CountDownLatch completeLatch;
  private boolean failure;

  /**
   * Create the sentinel to montior the given snapshot on the table
   * 
   * @param hsd
   * @param master
   * @param list
   * @param tableSnapshotRegions
   * @throws IOException
   */
  SnapshotSentinel(final SnapshotDescriptor hsd, final HMaster master,
      final MasterSnapshotStatusListener listener, List<ServerName> list)
      throws IOException {
    this.maxWait = master.getConfiguration().getLong(MASTER_SNAPSHOT_MAX_WAIT_TIME,
      DEFAULT_MAX_WAIT_TIME);
    this.wakeFrequency = master.getConfiguration().getInt(MAX_WAIT_FREQUENCY,
      DEFAULT_MAX_WAIT_FREQUENCY);
    this.hsd = hsd;
    this.status = GlobalSnapshotStatus.INIT;

    // setup the expected servers to join this snapshot
    this.waitToPrepare = list;
    this.prepareLatch = new CountDownLatch(list.size());
    this.completeLatch = new CountDownLatch(list.size());

    this.progressListener = listener;
  }

  /**
   * Indicate that the sentinel should consider this snapshot aborted
   */
  public void abort() {
    LOG.error("Aborting snapshot!");
    this.failure = true;
    this.setStatus(GlobalSnapshotStatus.ABORTING);
  }

  public void serverJoinedSnapshot(String rsID) {
    LOG.debug("server:" + rsID + " joined snapshot on sentinel.");
    if (this.waitToPrepare.remove(ServerName.parseServerName(rsID))) {
      this.prepareLatch.countDown();
      this.waitToFinish.add(rsID);
      // now check the length to see if we are done
      if (this.waitToPrepare.size() > 0) {
        setStatus(GlobalSnapshotStatus.RS_PREPARING_SNAPSHOT);
      }
    } else {
      LOG.warn("Server: " + rsID + " joined snapshot, but we weren't waiting on it to join.");
    }
  }

  public void serverCompletedSnapshot(String rsID) {
    if (this.waitToFinish.remove(rsID)) {
      this.completeLatch.countDown();
    } else {
      LOG.warn("Server: " + rsID + " joined snapshot, but we weren't waiting on it to join.");
    }
  }

  /** @return the snapshot descriptor for this snapshot */
  SnapshotDescriptor getSnapshot() {
    return this.hsd;
  }

  /**
   * Kickoff the snapshot and wait for the regionservers to finish.
   * @throws SnapshotCreationException if the snapshot could not be created
   */
  public void run() throws SnapshotCreationException {
    // wait on the prepare phase
    long now = EnvironmentEdgeManager.currentTimeMillis();
    while (waitToPrepare.size() > 0) {
      try {
        this.prepareLatch.await(wakeFrequency, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        // Ignore interruptions
      }
      // check to make sure we should go on
      checkFailure(now);
    }
    // all the regions have prepared, so now wait on completion
    this.setStatus(GlobalSnapshotStatus.RS_COMMITTING_SNAPSHOT);
    this.progressListener.allServersPreparedSnapshot();
    while (waitToFinish.size() > 0) {
      try {
        this.completeLatch.await(wakeFrequency, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        // Ignore interruptions
      }
      // check to make sure we should go on
      checkFailure(now);
    }
    this.setStatus(GlobalSnapshotStatus.ALL_RS_FINISHED);
    LOG.info("Done waiting - snapshot finished!");
  }

  private void checkFailure(long now) throws SnapshotCreationException {
    if (this.checkExceedsTimeout(now) || this.failure) {
      LOG.error("Snapshot failed - quiting sentinel");
      throw new SnapshotCreationException("Snapshot could not be created!", hsd);
    }
  }


  private boolean checkExceedsTimeout(long start) {
    long current = EnvironmentEdgeManager.currentTimeMillis();
    if ((current - start) > maxWait) {
      LOG.debug("Max wait (" + maxWait + ") exceeded wait time (current - start)");
      return true;
    }
    return false;
}

  /** @return status of the snapshot */
  GlobalSnapshotStatus getStatus() {
    return this.status;
  }

  private void setStatus(GlobalSnapshotStatus status) {
    this.status = status;
    logRegionServerProgress();
  }

  private void logRegionServerProgress() {
    LOG.debug("Waiting on snapshot: " + hsd + " to finish...");
    LOG.debug("Currently in phase:" + this.getStatus());
    LOG.debug("\t expecting to prepare:" + this.waitToPrepare);
    LOG.debug("\t expecting to finish:" + this.waitToFinish);
  }


  @Override
  public String toString() {
    return hsd.toString();
  }
}
