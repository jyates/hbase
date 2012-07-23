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
package org.apache.hadoop.hbase.master.handler;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.snapshot.RegionSnapshotUtils;
import org.apache.hadoop.hbase.regionserver.snapshot.operation.TableInfoCopyOperation;
import org.apache.hadoop.hbase.regionserver.snapshot.operation.WALReferenceOperation;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;
import org.apache.hadoop.hbase.snapshot.monitor.SnapshotFailureMonitor;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.zookeeper.KeeperException;

/**
 * Take a snapshot of a disabled table.
 */
public class DisabledTableSnapshotHandler extends TableEventHandler {
  private static final Log LOG = LogFactory.getLog(DisabledTableSnapshotHandler.class);
  private final SnapshotDescriptor snapshot;
  private final Configuration conf;
  private final FileSystem fs;
  private final Path rootDir;
  private final Path tdir;
  private final PathFilter dirFilter;
  private final PathFilter familyDirectory;
  private final PathFilter fileFilter;
  private SnapshotFailureMonitor errorMonitor;

  /**
   * @param snapshot descriptor of the snapshot to take
   * @param server parent server
   * @param masterServices master services provider
   * @param errorMonitor monitor the health of the snapshot
   * @throws IOException on unexpected error
   */
  public DisabledTableSnapshotHandler(SnapshotDescriptor snapshot, Server server,
      final MasterServices masterServices, SnapshotFailureMonitor errorMonitor) throws IOException {
    super(EventType.C_M_SNAPSHOT_TABLE, snapshot.getTableName(), server, masterServices);
    // The next call fails if no such table.
    getTableDescriptor();

    // complete the rest of the setup
    this.snapshot = snapshot;
    this.errorMonitor = errorMonitor;
    this.conf = this.masterServices.getConfiguration();
    this.fs = this.masterServices.getMasterFileSystem().getFileSystem();
    this.rootDir = FSUtils.getRootDir(this.conf);
    this.tdir = HTableDescriptor.getTableDir(this.rootDir, this.tableName);
    this.dirFilter = new FSUtils.DirFilter(this.fs);
    // only accept directory that don't start with "."
    this.familyDirectory = new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return dirFilter.accept(path) && !path.getName().startsWith(".");
      }
    };
    // !directory
    this.fileFilter = new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return !dirFilter.accept(path);
      }
    };
  }

  @Override
  protected void handleTableOperation(List<HRegionInfo> regions) throws IOException,
      KeeperException {
    // 1. for each region, write all the info to disk
    LOG.info("Starting to write region info and WALs for regions for offline snapshot:" + snapshot);
    for (HRegionInfo regionInfo : regions) {
      // 1.1 copy the regionInfo files to the snapshot
      Path snapshotRegionDir = RegionSnapshotUtils.getRegionSnaphshotDirectory(snapshot, rootDir,
        regionInfo.getEncodedName());
      HRegion.writeRegioninfoOnFilesystem(regionInfo, snapshotRegionDir, fs, conf);
      // check for error for each region
      errorMonitor.failOnError();

      // 1.2 create references for each of the WAL files for the region
      Path editsdir = HLog.getRegionDirRecoveredEditsDir(HRegion.getRegionDir(tdir,
        regionInfo.getEncodedName()));
      WALReferenceOperation op = new WALReferenceOperation(snapshot, errorMonitor, editsdir, conf,
          fs, "disabledTableSnapshot");
      op.run();
      errorMonitor.failOnError();
    }

    // 2. write the table info to disk
    LOG.info("Starting to copy tableinfo for offline snapshot:" + snapshot);
    TableInfoCopyOperation tableInfo = new TableInfoCopyOperation(this.errorMonitor, snapshot, fs,
        FSUtils.getRootDir(conf));
    tableInfo.run();
    errorMonitor.failOnError();

    // 3. reference all server directories for the table
    LOG.info("Starting to reference hfiles for offline snapshot:" + snapshot);
    // get the server directories
    FileStatus [] regionDirs = FSUtils.listStatus(fs, tdir,dirFilter);
    // if no regions, then we are done
    if (regionDirs == null || regionDirs.length == 0) return;
    errorMonitor.failOnError();

    // 4.1 for each region, reference the hfiles in that directory
    LOG.info("Referencing HFiles for offline snapshot:" + snapshot);
    for(FileStatus regionDir: regionDirs){
      FileStatus[] fams = FSUtils.listStatus(fs, regionDir.getPath(), familyDirectory);
      // if no families, then we are done again
      if (fams == null || fams.length == 0 ) continue;
      addReferencesToHFilesInRegion(regionDir.getPath(), fams);
      errorMonitor.failOnError();
    }
  }

  /**
   * Archive all the hfiles in the region
   * @param regionDir full path of the directory for the region
   * @param families status of all the families in the region
   * @throws IOException if we cannot create a reference or read a directory (underlying fs error)
   */
  private void addReferencesToHFilesInRegion(Path regionDir, FileStatus[] families) throws IOException {
    Path snapshotRegionDir = RegionSnapshotUtils.getRegionSnaphshotDirectory(snapshot, rootDir,
      regionDir.getName());
    for (FileStatus family : families) {
      // build the reference directory name
      Path dstStoreDir = RegionSnapshotUtils.getStoreSnapshotDirectory(snapshotRegionDir, family
          .getPath().getName());

      // get all the hfiles in the family
      FileStatus[] hfiles = FSUtils.listStatus(fs, family.getPath(), fileFilter);

      // if no hfiles, then we are done with this family
      if (hfiles == null || hfiles.length == 0) continue;

      // create a reference for each hfile
      for (FileStatus hfile : hfiles) {
        RegionSnapshotUtils.createReference(fs, conf, hfile.getPath(), dstStoreDir);
      }
    }
  }

}
