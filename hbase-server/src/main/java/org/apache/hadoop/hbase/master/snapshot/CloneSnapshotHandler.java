/**
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
import java.util.List;
import java.util.concurrent.CancellationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NotAllMetaRegionsOnlineException;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.SnapshotSentinel;
import org.apache.hadoop.hbase.master.handler.CreateTableHandler;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Handler to Clone a snapshot.
 *
 * <p>Uses {@link RestoreSnapshotHelper} to create a new table with the same
 * content of the specified snapshot.
 */
@InterfaceAudience.Private
public class CloneSnapshotHandler extends CreateTableHandler implements SnapshotSentinel {
  private static final Log LOG = LogFactory.getLog(CloneSnapshotHandler.class);

  private final static String NAME = "Master CloneSnapshotHandler";

  private final SnapshotDescription snapshot;

  private final ForeignExceptionDispatcher monitor;

  private volatile boolean stopped = false;

  private MonitoredTask status;

  public CloneSnapshotHandler(final MasterServices masterServices,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
      throws NotAllMetaRegionsOnlineException, TableExistsException, IOException {
    super(masterServices, masterServices.getMasterFileSystem(), hTableDescriptor,
      masterServices.getConfiguration(), null, masterServices.getCatalogTracker(),
      masterServices.getAssignmentManager());

    // Snapshot information
    this.snapshot = snapshot;

    // Monitor
    this.monitor = new ForeignExceptionDispatcher();
    this.status = TaskMonitor.get().createStatus("Cloning  snapshot '" + snapshot.getName() + "' to table "
      + hTableDescriptor.getNameAsString());
  }

  @Override
  protected List<HRegionInfo> handleCreateRegions(String tableName) throws IOException {
    status.setStatus("Creating regions for table: " + tableName);
    FileSystem fs = fileSystemManager.getFileSystem();
    Path rootDir = fileSystemManager.getRootDir();
    Path tableDir = HTableDescriptor.getTableDir(rootDir, Bytes.toBytes(tableName));

    try {
      // Execute the Clone
      Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshot, rootDir);
      RestoreSnapshotHelper restoreHelper = new RestoreSnapshotHelper(conf, fs, catalogTracker,
          snapshot, snapshotDir, hTableDescriptor, tableDir, monitor, status);
      restoreHelper.restore();

      // At this point the clone is complete. Next step is enabling the table.
      String msg = "Clone snapshot=" + snapshot.getName() + " on table=" + tableName
          + " completed!";
      status.setStatus(msg + " Waiting to table enabling...");
      LOG.info(msg);

      return MetaReader.getTableRegions(catalogTracker, Bytes.toBytes(tableName));
    } catch (Exception e) {
      String msg = "clone snapshot=" + snapshot + " failed because " + e.getMessage();
      LOG.error(msg, e);
      status.abort(msg);
      IOException rse = new RestoreSnapshotException(msg, e);

      // these handlers aren't futures so we need to register the error here.
      this.monitor.receive(new ForeignException(NAME, rse));
      throw rse;
    } finally {
      this.stopped = true;
    }
  }

  @Override
  public boolean isFinished() {
    return this.stopped;
  }

  @Override
  public SnapshotDescription getSnapshot() {
    return snapshot;
  }

  @Override
  public void cancel(String why) {
    if (this.stopped) return;
    this.stopped = true;
    String msg = "Stopping clone snapshot=" + snapshot + " because: " + why;
    LOG.info(msg);
    status.abort(msg);
    this.monitor.receive(new ForeignException(NAME, new CancellationException(why)));
  }

  @Override
  public ForeignException getExceptionIfFailed() {
    return this.monitor.getException();
  }
}
