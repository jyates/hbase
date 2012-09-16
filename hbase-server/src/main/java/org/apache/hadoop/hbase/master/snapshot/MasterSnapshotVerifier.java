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
import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription.Type;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.server.snapshot.TakeSnapshotUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.exception.CorruptedSnapshotException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;

/**
 * General snapshot verification on the master.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class MasterSnapshotVerifier {

  private SnapshotDescription snapshot;
  private FileSystem fs;
  private Path rootDir;
  private String tableName;
  private MasterServices services;

  /**
   * Build a util for the given snapshot
   * @param services services for the master
   * @param snapshot snapshot to check
   * @param rootDir root directory of the hbase installation.
   */
  public MasterSnapshotVerifier(MasterServices services, SnapshotDescription snapshot, Path rootDir) {
    this.fs = services.getMasterFileSystem().getFileSystem();
    this.services = services;
    this.snapshot = snapshot;
    this.rootDir = rootDir;
    this.tableName = snapshot.getTable();
  }

  /**
   * Verify that the snapshot in the directory is a valid snapshot
   * @param snapshotDir snapshot directory to check
   * @param snapshotServers {@link ServerName} of the servers that are involved in the snapshot
   * @throws CorruptedSnapshotException if the snapshot is invalid
   * @throws IOException if there is an unexpected connection issue to the filesystem
   */
  public void verifySnapshot(Path snapshotDir, Set<String> snapshotServers)
      throws CorruptedSnapshotException, IOException {
    // verify snapshot info matches
    verifySnapshotDescription(snapshotDir);

    // check that tableinfo is a valid table description
    verifyTableInfo(snapshotDir);

    // check that each region is valid
    verifyRegions(snapshotDir);

    // check that the hlogs, if they exist, are valid
    if (shouldCheckLogs(snapshot.getType())) {
      verifyLogs(snapshotDir, snapshotServers);
    }
  }

  /**
   * Check to see if the snapshot should verify the logs directory based on the type of the logs.
   * @param type type of snapshot being taken
   * @return <tt>true</tt> if the logs directory should be verified, <tt>false</tt> otherwise
   */
  private boolean shouldCheckLogs(Type type) {
    // This is better handled in the Type enum via type, but since its PB based, this is the
    // simplest way to handle it
    return !type.equals(Type.TIMESTAMP_VALUE);
  }

  /**
   * Check that the snapshot description written in the filesystem matches the current snapshot
   * @param snapshotDir snapshot directory to check
   */
  private void verifySnapshotDescription(Path snapshotDir) throws CorruptedSnapshotException {
    SnapshotDescription found = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
    if (!this.snapshot.equals(found)) {
      throw new CorruptedSnapshotException("Snapshot read (" + found
          + ") doesn't equal snapshot we ran (" + snapshot + ").");
    }
  }

  /**
   * Check that the table descriptor for the snapshot is a valid table descriptor
   * @param snapshotDir snapshot directory to check
   */
  private void verifyTableInfo(Path snapshotDir) throws IOException {
    FSTableDescriptors.getTableDescriptor(fs, snapshotDir);
  }

  /**
   * Check that all the regions in the the snapshot are valid
   * @param snapshotDir snapshot directory to check
   * @throws IOException if we can't reach .META. or read the files from the FS
   */
  private void verifyRegions(Path snapshotDir) throws IOException {
    List<HRegionInfo> regions = MetaReader.getTableRegions(this.services.getCatalogTracker(),
      Bytes.toBytes(tableName));
    for (HRegionInfo region : regions) {
      verifyRegion(fs, snapshotDir, region);
    }
  }

  /**
   * Verify that the region (regioninfo, hfiles) are valid
   * @param snapshotDir snapshot directory to check
   * @param region the region to check
   */
  private void verifyRegion(FileSystem fs, Path snapshotDir, HRegionInfo region) throws IOException {
    // make sure we have region in the snapshot
    Path regionDir = new Path(snapshotDir, region.getEncodedName());
    if (!fs.exists(regionDir)) {
      throw new CorruptedSnapshotException("No region directory found for region:" + region);
    }
    // make sure we have the region info in the snapshot
    Path regionInfo = new Path(regionDir, HRegion.REGIONINFO_FILE);
    // make sure the file exists
    if (!fs.exists(regionInfo)) {
      throw new CorruptedSnapshotException("No region info found for region:" + region);
    }
    FSDataInputStream in = fs.open(regionInfo);
    HRegionInfo found = HRegionInfo.parseFrom(in);
    if (!region.equals(found)) {
      throw new CorruptedSnapshotException("Found region info (" + found
          + ") doesn't match know region:" + region);
    }

    // make sure we have the expected recovered edits files
    TakeSnapshotUtils.verifyRecoveredEdits(fs, snapshotDir, found, snapshot);

    // check for the existance of each hfile
    PathFilter dir = new FSUtils.VisibleDirectory(fs);
    FileStatus[] columnFamilies = FSUtils.listStatus(fs, regionDir, dir);
    // should we do some checking here to make sure the cfs are correct?
    if (columnFamilies == null || columnFamilies.length == 0) return;

    // setup the postfixes
    Path tablePostFix = new Path(tableName);
    Path regionPostFix = new Path(tablePostFix, region.getEncodedName());

    // get the potential real paths
    Path archivedRegion = new Path(HFileArchiveUtil.getArchivePath(services.getConfiguration()),
        regionPostFix);
    Path realRegion = new Path(rootDir, regionPostFix);

    // loop through each cf and check we can find each of the hfiles
    for (FileStatus cf : columnFamilies) {
      FileStatus[] hfiles = FSUtils.listStatus(fs, cf.getPath(), null);
      // should we check if there should be hfiles?
      if (hfiles == null || hfiles.length == 0) continue;

      Path realCfDir = new Path(realRegion, cf.getPath().getName());
      Path archivedCfDir = new Path(archivedRegion, cf.getPath().getName());
      for (FileStatus hfile : hfiles) {
        // make sure the name is correct
        if (!StoreFile.validateStoreFileName(hfile.getPath().getName())) {
          throw new CorruptedSnapshotException("HFile: " + hfile.getPath()
              + " is not a valid hfile name.");
        }

        // check to see if hfile is present in the real table
        String fileName = hfile.getPath().getName();
        Path file = new Path(realCfDir, fileName);
        Path archived = new Path(archivedCfDir, fileName);
        if (!fs.exists(file) && !fs.equals(archived)) {
          throw new CorruptedSnapshotException("Cann't find hfile: " + hfile.getPath()
              + " in the real (" + archivedCfDir + ") or archive (" + archivedCfDir
              + ") directory for the primary table.");
        }
      }
    }
  }

  /**
   * Check that the logs stored in the log directory for the snapshot are valid - it contains all
   * the expected logs for all servers involved in the snapshot.
   * @param snapshotDir snapshot directory to check
   * @param snapshotServers list of the names of servers involved in the snapshot.
   * @throws CorruptedSnapshotException if the hlogs in the snapshot are not correct
   * @throws IOException if we can't reach the filesystem
   */
  private void verifyLogs(Path snapshotDir, Set<String> snapshotServers)
      throws CorruptedSnapshotException, IOException {
    Path snapshotLogDir = new Path(snapshotDir, HConstants.HREGION_LOGDIR_NAME);
    Path logsDir = new Path(rootDir, HConstants.HREGION_LOGDIR_NAME);
    TakeSnapshotUtils.verifyAllLogsGotReferenced(fs, logsDir, snapshotServers, snapshot,
      snapshotLogDir);
  }
}