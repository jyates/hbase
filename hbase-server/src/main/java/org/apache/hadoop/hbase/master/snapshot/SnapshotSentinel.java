/**
 * Copyright 2010 The Apache Software Foundation
 *
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.regionserver.snapshot.exception.SnapshotTimeoutException;
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
  private Object statusNotifier = new Object();


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

    // create the base directory for this snapshot
    MasterFileSystem mfs = master.getMasterFileSystem();
    Path snapshotDir = SnapshotDescriptor.getSnapshotDir(hsd.getSnapshotName(), mfs.getRootDir());
    if (!mfs.getFileSystem().mkdirs(snapshotDir)) {
      throw new IOException("Could not create snapshot directory: " + snapshotDir);
    }

    // setup the expected servers to join this snapshot
    this.waitToPrepare = list;

    this.progressListener = listener;

    // dump snapshot info
    SnapshotDescriptor.write(hsd, snapshotDir, mfs.getFileSystem());
  }

  /**
   * Indicate that the sentinel should consider this snapshot aborted
   */
  public void abort() {
    LOG.error("Aborting snapshot!");
    this.setStatus(GlobalSnapshotStatus.ABORTING);
  }

  public void serverJoinedSnapshot(String rsID) {
    if (this.waitToPrepare.remove(ServerName.parseServerName(rsID))) {
      this.waitToFinish.add(rsID);
      // now check the length to see if we are done
      if (this.waitToPrepare.size() == 0) {
        this.setStatus(GlobalSnapshotStatus.RS_FINISHING_SNAPSHOT);
        this.progressListener.allServersPreparedSnapshot();
      }
    } else {
      LOG.warn("Server: " + rsID + " joined snapshot, but we weren't waiting on it to join.");
    }
  }

  public void serverCompletedSnapshot(String rsID) {
    if (this.waitToFinish.remove(ServerName.parseServerName(rsID))) {
      // now check the length to see if we are done
      if (this.waitToFinish.size() == 0) {
        this.setStatus(GlobalSnapshotStatus.ALL_RS_READY);
      }
    } else {
      LOG.warn("Server: " + rsID + " joined snapshot, but we weren't waiting on it to join.");
    }
  }

  /** @return status of the snapshot */
  GlobalSnapshotStatus getStatus() {
    return this.status;
  }

  private void setStatus(GlobalSnapshotStatus status) {
    if (this.status == GlobalSnapshotStatus.ALL_RS_FINISHED) {
      LOG.warn("Snapshot attempting to abort after all the RS have finished.");
      return;
    }
    this.status = status;
    this.interruptWaiting();
  }

  /** @return the snapshot descriptor for this snapshot */
  SnapshotDescriptor getSnapshot() {
    return this.hsd;
  }

  /**
   * Kickoff the snapshot and wait for the regionservers to finish.
   * <p>
   * Timeout after maxRetries * 3000 ms. Increase maxRetries if snapshot always
   * timeout.
   * 
   * @throws SnapshotCreationException if timeout or snapshot is aborted or
   *           waiting is interrupted
   */
  public void waitToFinish() throws SnapshotCreationException {
    try {
      long now = EnvironmentEdgeManager.currentTimeMillis();

      synchronized (statusNotifier) {
        while (!status.equals(GlobalSnapshotStatus.ALL_RS_FINISHED)
            && !status.equals(GlobalSnapshotStatus.ABORTING)) {
          checkExceedsTimeout(now);
          LOG.debug("Waiting on snapshot: " + hsd + " to finish...");
          LOG.debug("\t expecting to prepare:" + this.waitToPrepare);
          LOG.debug("\t expecting to finish:" + this.waitToFinish);
          statusNotifier.wait(wakeFrequency);
        }
        if (status.equals(GlobalSnapshotStatus.ABORTING)) {
          throw new SnapshotCreationException("Snapshot is aborted: " + hsd);
        }
      }
    } catch (InterruptedException e) {
      throw new SnapshotCreationException("Master thread is interrupted for snapshot: " + hsd, e);
    }
  }

  private void checkExceedsTimeout(long start) throws SnapshotTimeoutException {
    long current = EnvironmentEdgeManager.currentTimeMillis();
    if ((current - start) > maxWait) {
      throw new SnapshotTimeoutException(maxWait, current - start);
    }
}

  void interruptWaiting() {
    synchronized (statusNotifier) {
      statusNotifier.notify();
    }
  }

  @Override
  public String toString() {
    return hsd.toString();
  }

  /**
   * Status of snapshot over the cluster. The end status of snapshot is
   * <code>ALL_RS_FINISHED</code> or <code>ABORTED</code>.
   */
  public static enum GlobalSnapshotStatus {
    INIT, // snapshot is just started over the cluster
    ALL_RS_READY, // all RS are ready for snapshot
    RS_FINISHING_SNAPSHOT, // RS are in process to complete snapshot
    ALL_RS_FINISHED, // all RS have finished the snapshot
    ABORTING; // abort the snapshot over the cluster
  }
}
