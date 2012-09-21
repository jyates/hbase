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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.server.commit.distributed.DistributedCommitException;
import org.apache.hadoop.hbase.server.commit.distributed.DistributedThreePhaseCommitErrorDispatcher;
import org.apache.hadoop.hbase.server.snapshot.error.SnapshotErrorMonitorFactory;
import org.apache.hadoop.hbase.server.snapshot.task.WALReferenceTask;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.exception.SnapshotCreationException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * Take a globally-consistent (stop-the-world) snapshot for a set of regions of a table on a
 * regionserver
 */
public class GlobalSnapshotOperation extends SnapshotOperation {

  private static final Log LOG = LogFactory.getLog(GlobalSnapshotOperation.class);

  private final HLog log;
  private final GlobalRegionSnapshotProgressMonitor progressMonitor;
  private final String serverName;

  public GlobalSnapshotOperation(DistributedThreePhaseCommitErrorDispatcher errorListener,
      long wakeFrequency, long timeout, List<HRegion> regions, SnapshotDescription snapshot,
      Configuration conf, SnapshotTaskManager taskManager,
      SnapshotErrorMonitorFactory monitorFactory, FileSystem fs, ServerName serverName) {
    super(errorListener, wakeFrequency, timeout, regions, snapshot, conf, taskManager,
        monitorFactory, fs);
    if (regions.size() <= 0) {
      throw new IllegalArgumentException("Must have at least one region to start a snapshot!");
    }
    // log for all regions is the same, so just pull the first one
    this.log = regions.get(0).getLog();
    this.progressMonitor = new GlobalRegionSnapshotProgressMonitor(regions.size(), wakeFrequency);
    this.serverName = serverName.toString();
  }

  @Override
  public void prepare() throws DistributedCommitException {
    // Start snapshotting all the involved regions
    try {
      // 1. create an operation for each region
      this.ops = new ArrayList<RegionSnapshotOperation>(regions.size());
      for (HRegion region : regions) {
        ops.add(new GloballyConsistentRegionLockTask(snapshot, region, snapshotErrorListener,
            progressMonitor, wakeFrequency));
      }

      // 2. submit those operations to the region snapshot runner
      for (RegionSnapshotOperation op : ops)
        taskManager.submitTask(op);

      // 2.1 do the copy table async at the same time
      snapshotErrorListener.failOnError();
      submitTableInfoCopy();

      // wait for all the regions to become stable (no more writes) so we can
      // put a reliable snapshot point in the WAL
      if (!progressMonitor.waitForRegionsToStabilize(snapshotErrorListener)) {
        // found an error, so just throw that
        snapshotErrorListener.failOnError();
      }

      // 3. append an edit in the WAL marking our position
      // append a marker for the snapshot to the walog
      // ensures the walog is synched to this point
      WALEdit edit = HLog.getSnapshotWALEdit();
      HRegionInfo regionInfo = new ServerRegionInfo(Bytes.toBytes(snapshot.getTable()));
      long now = EnvironmentEdgeManager.currentTimeMillis();
      this.log.append(regionInfo, Bytes.toBytes(snapshot.getTable()), edit, now, this.regions
          .get(0).getTableDesc());

      // 3.1 asynchronously add reference for the WALs
      taskManager.submitTask(new WALReferenceTask(snapshot, this.snapshotErrorListener, this.log
          .getDir(), conf, fs, this.serverName));

      // 4. Wait for WAL referencing to complete
      this.taskManager.waitForOutstandingTasks(this.getErrorCheckable());
    } catch (RejectedExecutionException e) {
      throw wrapExceptionForSnapshot(e);
    } catch (SnapshotCreationException e) {
      throw wrapExceptionForSnapshot(e);
    } catch (IOException e) {
      throw wrapExceptionForSnapshot(e);
    }
  }

  @Override
  public void commit() throws DistributedCommitException {
    // Release all the locks taken on the involved regions
    if (ops == null || ops.size() == 0) {
      LOG.debug("No region operations to release from the snapshot because we didn't get a chance"
          + " to create them.");
      return;
    }
    LOG.debug("Releasing commit barrier for globally consistent snapshot.");
    for (RegionSnapshotOperation op : ops) {
      ((GloballyConsistentRegionLockTask) op).getAllowCommitLatch().countDown();
    }

    // wait for all the outstanding tasks
    waitUntilDone();
  }

  /**
   * Wait for all the running tasks (as per the progress monitor) to finish.
   * @throws DistributedCommitException wrapped around any found exceptions while waiting for the
   *           progress monitor to complete
   */
  private void waitUntilDone() throws DistributedCommitException {
    if (!progressMonitor.waitUntilDone(this.snapshotErrorListener)) {
      try {
        this.snapshotErrorListener.failOnError();
      } catch (HBaseSnapshotException e) {
        throw wrapExceptionForSnapshot(e);
      }
    }
  }

  /**
   * Simple helper class that allows server-level meta edits to the WAL
   * <p>
   * These edits are NOT intended to be replayed on server failure or by the standard WAL player
   */
  private static class ServerRegionInfo extends HRegionInfo {
    private static final String MOCK_INFO_NAME_PREFIX = "_$NOT_A_REGION$_";

    public ServerRegionInfo(byte[] tablename) {
      super(tablename);
    }

    @Override
    public synchronized String getEncodedName() {
      return MOCK_INFO_NAME_PREFIX + super.getEncodedName();
    }

    @Override
    public boolean isMetaRegion() {
      return false;
    }
  }
}