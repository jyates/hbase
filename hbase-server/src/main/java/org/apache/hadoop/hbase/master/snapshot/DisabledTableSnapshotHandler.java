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

import javax.management.timer.Timer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.SnapshotHandler;
import org.apache.hadoop.hbase.master.snapshot.manage.SnapshotManager;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.server.errorhandling.notification.snapshot.SnapshotExceptionSnare;
import org.apache.hadoop.hbase.server.snapshot.TakeSnapshotUtils;
import org.apache.hadoop.hbase.server.snapshot.task.CopyRecoveredEditsTask;
import org.apache.hadoop.hbase.server.snapshot.task.ReferenceRegionHFilesTask;
import org.apache.hadoop.hbase.server.snapshot.task.ReferenceServerWALsTask;
import org.apache.hadoop.hbase.server.snapshot.task.TableInfoCopyTask;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.exception.SnapshotCreationException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Take a snapshot of a disabled table.
 * <p>
 * Table must exist when taking the snapshot, or results are undefined.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DisabledTableSnapshotHandler extends EventHandler implements SnapshotHandler {
  private static final Log LOG = LogFactory.getLog(DisabledTableSnapshotHandler.class);

  private volatile boolean finished = false;

  protected final Configuration conf;
  protected final FileSystem fs;
  protected final Path rootDir;

  private final SnapshotManager manager;
  private final Path snapshotDir;
  private final Path workingDir;

  private final PathFilter visibleDirFilter;
  private final Timer timer;

  private SnapshotDescription snapshot;

  private SnapshotExceptionSnare monitor;

  private MasterSnapshotVerifier verify;

  private MasterServices masterServices;

  private String tableName;

  private byte[] tableNameBytes;

  /**
   * @param snapshot descriptor of the snapshot to take
   * @param server parent server
   * @param masterServices master services provider
   * @param monitor monitor the health/progress of the snapshot
   * @param manager general manager for the snapshot
   * @throws IOException on unexpected error
   */
  public DisabledTableSnapshotHandler(SnapshotDescription snapshot, Server server,
      final MasterServices masterServices, SnapshotExceptionSnare monitor, SnapshotManager manager)
      throws IOException {
    super(server, EventType.C_M_SNAPSHOT_TABLE);
    this.masterServices = masterServices;
    this.tableName = snapshot.getTable();
    this.tableNameBytes = Bytes.toBytes(this.tableName);

    this.snapshot = snapshot;
    this.monitor = monitor;
    this.manager = manager;

    this.conf = this.masterServices.getConfiguration();
    this.fs = this.masterServices.getMasterFileSystem().getFileSystem();

    this.rootDir = FSUtils.getRootDir(this.conf);
    this.snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshot, rootDir);
    this.workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(snapshot, rootDir);
    this.visibleDirFilter = new FSUtils.VisibleDirectory(fs);

    // prepare the verify
    this.verify = new MasterSnapshotVerifier(masterServices, snapshot, rootDir);

    // setup the timer
    timer = new Timer();
  }

  // TODO consider parallelizing these operations since they are independent. Right now its just
  // easier to keep them serial though
  @Override
  public void process() {
    LOG.info("Running table snapshot operation " + eventType + " on table " + tableName);
    try {
      TakeSnapshotUtils.startMasterTimerAndBindToMonitor(timer, snapshot, conf, monitor);
      // write down the snapshot info in the working directory
      SnapshotDescriptionUtils.writeSnasphotInfo(snapshot, workingDir, this.fs);

      // 1. get all the regions hosting this table.
      List<Pair<HRegionInfo, ServerName>> regionsAndLocations = null;
      while (regionsAndLocations == null) {
        try {
          regionsAndLocations = MetaReader.getTableRegionsAndLocations(
            this.server.getCatalogTracker(), tableNameBytes, true);
        } catch (InterruptedException e) {
          // check to see if we failed, in which case return
          if (this.monitor.checkForException()) return;
          // otherwise, just reset the interrupt and keep on going
          Thread.currentThread().interrupt();
        }
      }

      // extract each pair to separate lists
      Set<String> serverNames = new HashSet<String>();
      Set<HRegionInfo> regions = new HashSet<HRegionInfo>();
      for (Pair<HRegionInfo, ServerName> p : regionsAndLocations) {
        regions.add(p.getFirst());
        serverNames.add(p.getSecond().toString());
      }

      // 2. for each region, write all the info to disk
      LOG.info("Starting to write region info and WALs for regions for offline snapshot:"
          + snapshot);
      for (HRegionInfo regionInfo : regions) {
        // 2.1 copy the regionInfo files to the snapshot
        Path snapshotRegionDir = TakeSnapshotUtils.getRegionSnaphshotDirectory(snapshot, rootDir,
          regionInfo.getEncodedName());
        HRegion.writeRegioninfoOnFilesystem(regionInfo, snapshotRegionDir, fs, conf);
        // check for error for each region
        monitor.failOnException();

        // 2.2 for each region, copy over its recovered.edits directory
        Path regionDir = HRegion.getRegionDir(rootDir, regionInfo);
        new CopyRecoveredEditsTask(snapshot, monitor, fs, regionDir, snapshotRegionDir).run();
        monitor.failOnException();

        // 2.3 reference all the files in the region
        new ReferenceRegionHFilesTask(snapshot, monitor, regionDir, fs, snapshotRegionDir).run();
        monitor.failOnException();
      }

      // 3. create references for each of the WAL files each server
      Path logdir = new Path(FSUtils.getRootDir(conf), HConstants.HREGION_LOGDIR_NAME);
      FileStatus[] serverLogDirs = FSUtils.listStatus(fs, logdir, visibleDirFilter);
      // if we have logs directories, then reference all the logs in each necessaryserver directory
      if (serverLogDirs != null) {
        for (FileStatus serverDir : serverLogDirs) {
          if (serverNames.contains(serverDir.getPath().getName())) {
            ReferenceServerWALsTask op = new ReferenceServerWALsTask(snapshot, this.monitor,
                serverDir.getPath(), conf, fs);
            op.run();
          }

          monitor.failOnException();
        }
      }

      // 4. write the table info to disk
      LOG.info("Starting to copy tableinfo for offline snapshot:\n" + snapshot);
      TableInfoCopyTask tableInfo = new TableInfoCopyTask(this.monitor, snapshot, fs,
          FSUtils.getRootDir(conf));
      tableInfo.run();
      monitor.failOnException();

      // verify the snapshot is valid
      verify.verifySnapshot(this.workingDir, serverNames);

      // complete the snapshot
      manager.completeSnapshot(this.snapshotDir, this.workingDir, this.fs);

      // mark the operation as complete
      timer.stop();
    } catch (Exception e) {
      // make sure we capture the exception to propagate back to the client later
      monitor.snapshotFailure(new SnapshotCreationException(e, snapshot));
    } finally {
      LOG.debug("Marking snapshot" + this.snapshot + " as finsihed.");
      this.finished = true;

      LOG.debug("Launching cleanup of working dir:" + workingDir);
      try {
        // don't mark the snapshot as a failure if we can't cleanup - the snapshot worked.
        if (!this.fs.delete(this.workingDir, true)) {
          LOG.error("Couldn't delete snapshot working directory:" + workingDir);
        }
      } catch (IOException e) {
        LOG.error("Couldn't delete snapshot working directory:" + workingDir);
      }
    }
  }

  @Override
  public boolean getFinished() {
    return this.finished;
  }

  @Override
  public SnapshotDescription getSnapshot() {
    return snapshot;
  }

  @Override
  public HBaseSnapshotException getExceptionIfFailed() {
    try {
      monitor.failOnException();
    } catch (HBaseSnapshotException e) {
      return e;
    }
    return null;
  }
}