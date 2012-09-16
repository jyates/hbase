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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.snapshot.manage.SnapshotManager;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.server.errorhandling.OperationAttemptTimer;
import org.apache.hadoop.hbase.server.snapshot.TakeSnapshotUtils;
import org.apache.hadoop.hbase.server.snapshot.error.SnapshotErrorListener;
import org.apache.hadoop.hbase.server.snapshot.task.ReferenceServerWALsTask;
import org.apache.hadoop.hbase.server.snapshot.task.TableInfoCopyTask;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.zookeeper.KeeperException;

/**
 * Take a snapshot of a disabled table.
 */
public class DisabledTableSnapshotHandler extends TableSnapshotHandler {
  private static final Log LOG = LogFactory.getLog(DisabledTableSnapshotHandler.class);

  private final Path tdir;
  private final PathFilter visibleDirFilter;
  private final PathFilter fileFilter;
  private final OperationAttemptTimer timer;

  /**
   * @param snapshot descriptor of the snapshot to take
   * @param server parent server
   * @param masterServices master services provider
   * @param monitor monitor the health/progress of the snapshot
   * @param manager general manager for the snapshot
   * @throws IOException on unexpected error
   */
  public DisabledTableSnapshotHandler(SnapshotDescription snapshot, Server server,
      final MasterServices masterServices, SnapshotErrorListener monitor, SnapshotManager manager)
      throws IOException {
    super(snapshot, server, masterServices, monitor, manager);
    this.tdir = HTableDescriptor.getTableDir(this.rootDir, this.tableName);
    this.visibleDirFilter = new FSUtils.VisibleDirectory(fs);
    // !directory
    this.fileFilter = new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return !visibleDirFilter.accept(path);
      }
    };
    // setup the timer
    timer = TakeSnapshotUtils.getMasterTimerAndBindToMonitor(snapshot, conf,
      monitor);
  }

  // TODO consider parallelizing these operations since they are independent. Right now its just
  // easier to keep them serial though
  @Override
  protected void snapshot(List<HRegionInfo> regions) throws IOException, KeeperException {
    // 0. start the timer for taking the snapshot
    timer.start();
    // 1. for each region, write all the info to disk
    LOG.info("Starting to write region info and WALs for regions for offline snapshot:" + snapshot);
    for (HRegionInfo regionInfo : regions) {
      // 1.1 copy the regionInfo files to the snapshot
      Path snapshotRegionDir = TakeSnapshotUtils.getRegionSnaphshotDirectory(snapshot, rootDir,
        regionInfo.getEncodedName());
      HRegion.writeRegioninfoOnFilesystem(regionInfo, snapshotRegionDir, fs, conf);
      // check for error for each region
      monitor.failOnError();

      // 1.2 create references for each of the WAL files for the region
      Path logdir = new Path(FSUtils.getRootDir(conf), HConstants.HREGION_LOGDIR_NAME);
      FileStatus[] serverLogDirs = FSUtils.listStatus(fs, logdir, visibleDirFilter);
      // if we have logs directories, then reference all the logs in each server directory
      if (serverLogDirs != null) {
        for (FileStatus serverDir : serverLogDirs) {
          ReferenceServerWALsTask op = new ReferenceServerWALsTask(snapshot, this.monitor,
              serverDir.getPath(), conf, fs);
          op.run();
          monitor.failOnError();
        }
      }
    }

    // 2. write the table info to disk
    LOG.info("Starting to copy tableinfo for offline snapshot:\n" + snapshot);
    TableInfoCopyTask tableInfo = new TableInfoCopyTask(this.monitor, snapshot, fs,
        FSUtils.getRootDir(conf));
    tableInfo.run();
    monitor.failOnError();

    // 3. reference all region directories for the table
    LOG.info("Starting to reference hfiles for offline snapshot:" + snapshot);
    // get the server directories
    FileStatus[] regionDirs = FSUtils.listStatus(fs, tdir, visibleDirFilter);
    // if no regions, then we are done
    if (regionDirs == null) return;
    monitor.failOnError();

    // 4.1 for each region, reference the hfiles in that directory
    if (LOG.isDebugEnabled()) {
      // log the directories we are searching for hfiles
      List<String> dirs = new ArrayList<String>(regionDirs.length);
      for (FileStatus files : regionDirs) {
        dirs.add(files.getPath().toString());
      }
      LOG.info("Referencing HFiles in " + dirs + " for offline snapshot:" + snapshot);
    }
    for (FileStatus regionDir : regionDirs) {
      FileStatus[] fams = FSUtils.listStatus(fs, regionDir.getPath(), visibleDirFilter);
      // if no families, then we are done again
      if (fams == null || fams.length == 0) continue;
      addReferencesToHFilesInRegion(regionDir.getPath(), fams);
      monitor.failOnError();
    }

    // 5. mark operation as completed
    timer.complete();
  }

  /**
   * Archive all the hfiles in the region
   * @param regionDir full path of the directory for the region
   * @param families status of all the families in the region
   * @throws IOException if we cannot create a reference or read a directory (underlying fs error)
   */
  private void addReferencesToHFilesInRegion(Path regionDir, FileStatus[] families)
      throws IOException {
    if (families == null || families.length == 0) {
      LOG.info("No families under region directory:" + regionDir
          + ", not attempting to add references.");
      return;
    }

    // snapshot directories to store the hfile reference
    List<Path> snapshotFamilyDirs = TakeSnapshotUtils.getFamilySnapshotDirectories(snapshot,
      rootDir, regionDir.getName(), families);

    LOG.debug("Add hfile references to snapshot directories:" + snapshotFamilyDirs);
    for (int i = 0; i < families.length; i++) {
      FileStatus family = families[i];
      Path familyDir = family.getPath();
      // get all the hfiles in the family
      FileStatus[] hfiles = FSUtils.listStatus(fs, familyDir, fileFilter);

      // if no hfiles, then we are done with this family
      if (hfiles == null || hfiles.length == 0) {
        LOG.debug("Not hfiles found for family: " + familyDir);
        continue;
      }

      // create a reference for each hfile
      for (FileStatus hfile : hfiles) {
        TakeSnapshotUtils.createReference(fs, conf, hfile.getPath(), snapshotFamilyDirs.get(i));
      }
    }
  }
}