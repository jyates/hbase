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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.server.commit.distributed.DistributedCommitException;
import org.apache.hadoop.hbase.server.commit.distributed.coordinator.CoordinatorTask;
import org.apache.hadoop.hbase.server.commit.distributed.coordinator.CoordinatorTaskBuilder;
import org.apache.hadoop.hbase.server.commit.distributed.coordinator.DistributedThreePhaseCommitCoordinator;
import org.apache.hadoop.hbase.server.snapshot.error.SnapshotErrorListener;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.collect.Lists;

/**
 * Handle the snapshot of an online table, regardless of snapshot table. Leverages a
 * {@link CoordinatorTask} to run the snapshot across all the involved regions.
 * @see DistributedThreePhaseCommitCoordinator
 */
@InterfaceAudience.Private
public class OnlineTableSnapshotHandler extends TableSnapshotHandler {

  private static final Log LOG = LogFactory.getLog(OnlineTableSnapshotHandler.class);

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

  private DistributedThreePhaseCommitCoordinator coordinator;

  public OnlineTableSnapshotHandler(SnapshotDescription snapshot, Server server,
      MasterServices master, SnapshotErrorListener monitor, SnapshotManager manager,
      long wakeFrequency) throws IOException {
    super(snapshot, server, master, monitor, manager);
    // set the current status
    this.status = GlobalSnapshotStatus.INIT;
    this.coordinator = manager.getCoordinator();
    long timeout = SnapshotDescriptionUtils.getMaxMasterTimeout(conf, snapshot.getType());
    // this is a little inefficient in terms of memory - we create a new task builder each time we
    // want to create a snapshot. However, its short-lived - only the length of the snapshot - and
    // is easier
    // to reason about when we want to do multiple concurrent snapshots.
    CoordinatorTaskBuilder coordinatorBuilder = new CoordinatorTaskBuilder(wakeFrequency, timeout,
        null);
    this.coordinator.setBuilder(coordinatorBuilder);
  }

  // TODO consider switching over to using regionnames, rather than server names. This would allow
  // regions to migrate during a snapshot, and then be involved when they are ready. Still want to
  // enforce a snapshot time constraints, but lets us to potentially be a bit more robust.

  @Override
  protected void snapshot(List<Pair<HRegionInfo, ServerName>> regions)
      throws HBaseSnapshotException {
    Set<String> tableRegions = new HashSet<String>(regions.size());
    for (Pair<HRegionInfo, ServerName> region : regions) {
      tableRegions.add(region.getSecond().toString());
    }

    // start the snapshot on the RS
    this.setStatus(GlobalSnapshotStatus.RS_PREPARING_SNAPSHOT);
    CoordinatorTask task = coordinator.kickOffCommit(this.snapshot.getName(),
      this.snapshot.toByteArray(), Lists.newArrayList(tableRegions));

    try {
      // wait on the prepared latch to count-down
      task.waitForLatch(task.getPreparedLatch(), "All servers prepared");
      LOG.debug("All servers have joined the snapshot, starting commit...");
      this.setStatus(GlobalSnapshotStatus.RS_COMMITTING_SNAPSHOT);

      // all the regions have prepared, so now wait on commits
      task.waitForLatch(task.getCommitFinishedLatch(), "All servers committed snapshot");
      this.setStatus(GlobalSnapshotStatus.ALL_RS_FINISHED);

      // wait for the snapshot to complete
      task.waitForLatch(task.getCompletedLatch(), "completed snapshot");
      LOG.info("Done waiting - snapshot finished!");
    } catch (InterruptedException e) {
      this.monitor.snapshotFailure(
        "Interrupted while waiting for snapshot to finish, current phase:" + status, getSnapshot(),
        e);
      Thread.currentThread().interrupt();
    } catch (DistributedCommitException e) {
      this.monitor.snapshotFailure("Failure while waiting for snapshot to finish, current phase:"
          + status, getSnapshot(), e);
    }
  }

  /** @return status of the snapshot */
  GlobalSnapshotStatus getStatus() {
    return this.status;
  }

  private void setStatus(GlobalSnapshotStatus status) {
    this.status = status;
    LOG.debug("Waiting on snapshot: " + this.snapshot + " to finish...");
    LOG.debug("Currently in phase:" + this.getStatus());
  }
}