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
package org.apache.hadoop.hbase.master.snapshot.manage;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.snapshot.monitor.DistributedSnapshotMonitor;
import org.apache.hadoop.hbase.master.snapshot.monitor.MasterSnapshotStatusListener;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;

/**
 * {@link SnapshotSentinel} that monitors a snapshot against a 'live' table.
 */
public class OnlineSnapshotSentinel extends SnapshotSentinel implements DistributedSnapshotMonitor {

  private static final Log LOG = LogFactory.getLog(OnlineSnapshotSentinel.class);

  /**
   * Status of snapshot over the cluster. The end status of snapshot is <code>ALL_RS_FINISHED</code>
   * or <code>ABORTED</code>.
   */
  public static enum GlobalSnapshotStatus {
    INIT, // snapshot just started over the cluster
    RS_PREPARING_SNAPSHOT, // some RS are still preparing the snapshot
    RS_COMMITTING_SNAPSHOT, // RS are committing the snapshot
    ALL_RS_FINISHED, // all RS have finished committing the snapshot
    ABORTING; // abort the snapshot over the cluster
  }

  // current status of snapshot
  private volatile GlobalSnapshotStatus status;

  /**
   * active servers when the snapshot is started. The table for snapshot is served by these servers
   * so snapshot is finished only if finished on all these region servers.
   */
  private List<ServerName> waitToPrepare;

  /**
   * The list of servers waiting to finish the snapshot.Servers that have finished preparing are
   * moved to waitToFinish.
   */
  private List<String> waitToFinish = new LinkedList<String>();

  private final MasterSnapshotStatusListener progressListener;
  private CountDownLatch prepareLatch;
  private CountDownLatch completeLatch;

  /**
   * @param hsd
   * @param master
   * @param listener
   * @param list
   * @throws IOException
   */
  OnlineSnapshotSentinel(SnapshotDescriptor hsd, MasterServices master,
      MasterSnapshotStatusListener listener, List<ServerName> list) throws IOException {
    super(hsd, master);
    this.status = GlobalSnapshotStatus.INIT;
    this.progressListener = listener;

    // setup the expected servers to join this snapshot
    this.waitToPrepare = list;
    this.prepareLatch = new CountDownLatch(list.size());
    this.completeLatch = new CountDownLatch(list.size());
  }

  @Override
  public void snapshot() throws SnapshotCreationException {
    // wait on the prepare phase
    while (waitToPrepare.size() > 0) {
      try {
        this.prepareLatch.await(wakeFrequency, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        // Ignore interruptions
      }
      // check to make sure we should go on
      failOnError();
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
      failOnError();
    }
    this.setStatus(GlobalSnapshotStatus.ALL_RS_FINISHED);
    LOG.info("Done waiting - snapshot finished!");
  }

  @Override
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

  @Override
  public void serverCompletedSnapshot(String rsID) {
    if (this.waitToFinish.remove(rsID)) {
      this.completeLatch.countDown();
    } else {
      LOG.warn("Server: " + rsID + " joined snapshot, but we weren't waiting on it to join.");
    }
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
    LOG.debug("Waiting on snapshot: " + this.getSnapshot() + " to finish...");
    LOG.debug("Currently in phase:" + this.getStatus());
    LOG.debug("\t expecting to prepare:" + this.waitToPrepare);
    LOG.debug("\t expecting to finish:" + this.waitToFinish);
  }
}