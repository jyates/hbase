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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.handler.TableEventHandler;
import org.apache.hadoop.hbase.master.snapshot.manage.SnapshotManager;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.restore.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.snapshot.exception.RestoreSnapshotException;
import org.apache.zookeeper.KeeperException;

/**
 * Handler to Restore a snapshot.
 */
@InterfaceAudience.Private
public class RestoreSnapshotHandler extends TableEventHandler {
  private static final Log LOG = LogFactory.getLog(RestoreSnapshotHandler.class);

  private final HTableDescriptor hTableDescriptor;
  private final SnapshotManager snapshotManager;
  private final SnapshotDescription snapshot;
  private final long waitTime;

  public RestoreSnapshotHandler(final MasterServices masterServices,
      final SnapshotManager snapshotManager,
      final SnapshotDescription snapshot, final HTableDescriptor htd, long waitTime)
      throws IOException {
    super(EventType.C_M_RESTORE_SNAPSHOT, htd.getName(), masterServices, masterServices);

    // Snapshot information
    this.snapshotManager = snapshotManager;
    this.snapshot = snapshot;
    this.waitTime = waitTime;

    // Check table exists.
    getTableDescriptor();

    // This is the new schema we are going to write out as this modification.
    this.hTableDescriptor = htd;
  }

  @Override
  protected void handleTableOperation(List<HRegionInfo> hris) throws IOException {
    try {
      // Update descriptor
      this.masterServices.getTableDescriptors().add(hTableDescriptor);

      Configuration conf = masterServices.getConfiguration();
      CatalogTracker catalogTracker = masterServices.getCatalogTracker();
      MasterFileSystem fileSystemManager = masterServices.getMasterFileSystem();
      FileSystem fs = fileSystemManager.getFileSystem();
      Path rootDir = fileSystemManager.getRootDir();
      Path tableDir = HTableDescriptor.getTableDir(rootDir, tableName);
      Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshot, rootDir);

      byte[] tableName = hTableDescriptor.getName();
      RestoreSnapshotHelper restoreHelper = new RestoreSnapshotHelper(conf, fs,
          catalogTracker, snapshot, snapshotDir, hTableDescriptor, tableDir, waitTime);
      restoreHelper.restore();

      // At this point the restore is complete. Next step is enabling the table.
      snapshotManager.restoreCompleted(snapshot);

      hris.clear();
      hris.addAll(MetaReader.getTableRegions(catalogTracker, tableName));
    } catch (IOException e) {
      snapshotManager.restoreFailed(snapshot, e);
      throw new RestoreSnapshotException(e);
    }

    // Enable the table
    try {
      handleEnableTable(hris);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  private void handleEnableTable(final List<HRegionInfo> regions)
      throws IOException, KeeperException {
    AssignmentManager assignmentManager = masterServices.getAssignmentManager();

    // 1. Trigger immediate assignment of the regions in round-robin fashion
    try {
      assignmentManager.getRegionStates().createRegionStates(regions);
      assignmentManager.assign(regions);
    } catch (InterruptedException ie) {
      LOG.error("Caught " + ie + " during round-robin assignment");
      throw new IOException(ie);
    }

    // 2. Set table enabled flag up in zk.
    try {
      assignmentManager.getZKTable().
        setEnabledTable(this.hTableDescriptor.getNameAsString());
    } catch (KeeperException e) {
      throw new IOException("Unable to ensure that the table will be" +
        " enabled because of a ZooKeeper issue", e);
    }
  }
}
