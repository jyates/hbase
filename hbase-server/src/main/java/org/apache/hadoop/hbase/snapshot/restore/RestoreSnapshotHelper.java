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

package org.apache.hadoop.hbase.snapshot.restore;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.snapshot.exception.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.FSVisitor;

/**
 * Helper to Restore/Clone a Snapshot
 *
 * <p>The helper assumes that a table is already created, and by calling restore()
 * the content present in the snapshot will be restored as the new content of the table.
 *
 * <p>Clone from Snapshot: If the target table is empty, the restore operation
 * translates in just a clone operation, where the only operations are:
 * <ul>
 *  <li>for each region in the snapshot create a new region
 *    (note that the region will have a different name, since the encoding contains the table name)
 *  <li>for each file in the region create a new HFileLink to point to the original file.
 *  <li>restore the logs, if any
 * </ul>
 *
 * <p>Restore from Snapshot:
 * <ul>
 *  <li>for each region in the table verify which are available in the snapshot and which are not
 *    <ul>
 *    <li>if the region is not present in the snapshot, remove it.
 *    <li>if the region is present in the snapshot
 *      <ul>
 *      <li>for each file in the table region verify which are available in the snapshot
 *        <ul>
 *          <li>if the hfile is not present in the snapshot, remove it
 *          <li>if the hfile is present, keep it (nothing to do)
 *        </ul>
 *      <li>for each file in the snapshot region but not in the table
 *        <ul>
 *          <li>create a new HFileLink that point to the original file
 *        </ul>
 *      </ul>
 *    </ul>
 *  <li>for each region in the snapshot not present in the current table state
 *    <ul>
 *    <li>create a new region and for each file in the region create a new HFileLink
 *      (This is the same as the clone operation)
 *    </ul>
 *  <li>restore the logs, if any
 * </ul>
 */
@InterfaceAudience.Private
public class RestoreSnapshotHelper {
	private static final Log LOG = LogFactory.getLog(RestoreSnapshotHelper.class);

  public static final String MASTER_WAIT_TIME_RESTORE_SNAPSHOT = "hbase.snapshot.restore.master.timeout";
  public static final long DEFAULT_MAX_WAIT_TIME = 60000;

  private final Map<byte[], byte[]> regionsMap =
        new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);

  private final SnapshotDescription snapshotDesc;
  private final Path snapshotDir;

  private final HTableDescriptor tableDesc;
  private final Path tableDir;

  private final CatalogTracker catalogTracker;
  private final Configuration conf;
  private final FileSystem fs;

  private final long waitTime;

  public RestoreSnapshotHelper(final Configuration conf, final FileSystem fs,
      final CatalogTracker catalogTracker,
      final SnapshotDescription snapshotDescription, final Path snapshotDir,
      final HTableDescriptor tableDescriptor, final Path tableDir)
  {
    this(conf, fs, catalogTracker, snapshotDescription, snapshotDir,
      tableDescriptor, tableDir, 0);
  }

  public RestoreSnapshotHelper(final Configuration conf, final FileSystem fs,
      final CatalogTracker catalogTracker,
      final SnapshotDescription snapshotDescription, final Path snapshotDir,
      final HTableDescriptor tableDescriptor, final Path tableDir, long waitTime)
  {
    this.fs = fs;
    this.conf = conf;
    this.catalogTracker = catalogTracker;
    this.snapshotDesc = snapshotDescription;
    this.snapshotDir = snapshotDir;
    this.tableDesc = tableDescriptor;
    this.tableDir = tableDir;
    this.waitTime = waitTime;
  }

  /**
   * Restore table to a specified snapshot state.
   */
  public void restore() throws IOException {
    restore(false);
  }

  /**
   * Restore table to a specified snapshot state.
   *
   * @param noWALs Avoid restoring WALs
   */
  public void restore(boolean noWALs) throws IOException {
    long startTime = EnvironmentEdgeManager.currentTimeMillis();

    Set<String> snapshotRegionNames = SnapshotReferenceUtil.getSnapshotRegionNames(fs, snapshotDir);
    if (snapshotRegionNames == null) {
      LOG.warn("Nothing to restore. Snapshot " + snapshotDesc + " looks empty");
      return;
    }

    // Identify which region are still available and which not.
    // NOTE: we rely upon the region name as: "table name, start key, end key"
    List<HRegionInfo> tableRegions = getTableRegions();
    if (tableRegions != null) {
      isTimeElapsed(startTime);
      List<HRegionInfo> regionsToRestore = new LinkedList<HRegionInfo>();
      List<HRegionInfo> regionsToRemove = new LinkedList<HRegionInfo>();

      for (HRegionInfo regionInfo: tableRegions) {
        String regionName = regionInfo.getEncodedName();
        if (snapshotRegionNames.contains(regionName)) {
          LOG.info("region to restore: " + regionName);
          snapshotRegionNames.remove(regionInfo);
          regionsToRestore.add(regionInfo);
        } else {
          LOG.info("region to remove: " + regionName);
          regionsToRemove.add(regionInfo);
        }
      }

      // Restore regions using the snapshot data
      isTimeElapsed(startTime);
      restoreRegions(regionsToRestore);

      // Remove regions from the current table
      isTimeElapsed(startTime);
      removeRegions(regionsToRemove);
    }

    // Regions to Add: present in the snapshot but not in the current table
    if (snapshotRegionNames.size() > 0) {
      List<HRegionInfo> regionsToAdd = new LinkedList<HRegionInfo>();

      isTimeElapsed(startTime);
      for (String regionName: snapshotRegionNames) {
        LOG.info("region to add: " + regionName);
        Path regionDir = new Path(snapshotDir, regionName);
        regionsToAdd.add(HRegion.loadDotRegionInfoFileContent(fs, regionDir));
      }

      // Create new regions cloning from the snapshot
      isTimeElapsed(startTime);
      cloneRegions(regionsToAdd);
    }

    // Restore WALs
    if (!noWALs) {
      isTimeElapsed(startTime);
      restoreWALs();
    } else {
      LOG.info("Skip restore WALs for " + snapshotDesc.getName());
    }
  }

  /**
   * Throw an exception if the waitTime for the restore is elapsed
   */
  private void isTimeElapsed(long startTime) throws IOException {
    long elapsedTime = EnvironmentEdgeManager.currentTimeMillis() - startTime;

    LOG.debug("restore snapshot=" + snapshotDesc.getName() +
      " on table=" + snapshotDesc.getTable() +
      " elapsed=" + StringUtils.formatTime(elapsedTime));
    if (waitTime != 0 && elapsedTime > waitTime) {
      throw new RestoreSnapshotException(
        "Restore time exceeded the expectation elapsedTime=" + elapsedTime);
    }
  }

  /**
   * Restore specified regions by restoring content to the snapshot state.
   */
  private void restoreRegions(final List<HRegionInfo> regions) throws IOException {
    if (regions == null || regions.size() == 0) return;
    for (HRegionInfo hri: regions) restoreRegion(hri);
  }

  /**
   * Restore region by removing files not it in the snapshot
   * and adding the missing ones from the snapshot.
   */
  private void restoreRegion(HRegionInfo regionInfo) throws IOException {
    Path snapshotRegionDir = new Path(snapshotDir, regionInfo.getEncodedName());
    Map<String, List<String>> snapshotFiles =
                SnapshotReferenceUtil.getRegionHFileReferences(fs, snapshotRegionDir);

    Path regionDir = new Path(tableDir, regionInfo.getEncodedName());
    String tableName = tableDesc.getNameAsString();

    for (Map.Entry<String, List<String>> familyEntry: snapshotFiles.entrySet()) {
      byte[] family = Bytes.toBytes(familyEntry.getKey());
      Path familyDir = new Path(regionDir, familyEntry.getKey());
      Set<String> familyFiles = getTableRegionFamilyFiles(familyDir);

      List<String> hfilesToAdd = new LinkedList<String>();
      for (String hfileName: familyEntry.getValue()) {
        if (familyFiles.contains(hfileName)) {
          // HFile already present
          familyFiles.remove(hfileName);
        } else {
          // HFile missing
          hfilesToAdd.add(hfileName);
        }
      }

      // Remove hfiles not present in the snapshot
      for (String hfileName: familyFiles) {
        Path hfile = new Path(familyDir, hfileName);
        LOG.trace("Removing hfile=" + hfile + " from table=" + tableName);
        HFileArchiver.archiveStoreFile(fs, regionInfo, conf, tableDir, family, hfile);
      }

      // Restore Missing files
      for (String hfileName: hfilesToAdd) {
        LOG.trace("Adding HFileLink " + hfileName + " to table=" + tableName);
        HFileLink.create(conf, fs, familyDir, regionInfo, hfileName);
      }
    }
  }

  /**
   * @return The set of files in the specified family directory.
   */
  private Set<String> getTableRegionFamilyFiles(final Path familyDir) throws IOException {
    Set<String> familyFiles = new HashSet<String>();

    FileStatus[] hfiles = FSUtils.listStatus(fs, familyDir);
    if (hfiles == null) return familyFiles;

    for (FileStatus hfileRef: hfiles) {
      String hfileName = hfileRef.getPath().getName();
      if (HFileLink.isHFileLink(hfileName)) {
        hfileName = HFileLink.getReferencedHFileName(hfileName);
      } else if (Reference.isReference(hfileRef.getPath())) {
        hfileName = Reference.getDeferencedHFileName(hfileName);
      }
      familyFiles.add(hfileName);
    }

    return familyFiles;
  }

  /**
   * Remove specified regions by removing them from file-system and .META.
   */
  private void removeRegions(final List<HRegionInfo> regions) throws IOException {
    if (regions == null || regions.size() == 0) return;
    for (HRegionInfo hri: regions) removeRegion(hri);
  }

  /**
   * Remove region from file-system and .META.
   */
  private void removeRegion(HRegionInfo regionInfo) throws IOException {
    // Remove region from META
    MetaEditor.deleteRegion(catalogTracker, regionInfo);

    // "Delete" region from FS
    HFileArchiver.archiveRegion(fs, regionInfo);
  }

  /**
   * Clone specified regions. For each region create a new region
   * and create a HFileLink for each hfile.
   */
  private void cloneRegions(final List<HRegionInfo> regions) throws IOException {
    if (regions == null || regions.size() == 0) return;

    final int batchSize = conf.getInt("hbase.master.createtable.batchsize", 100);
    List<HRegionInfo> createdRegions = new ArrayList<HRegionInfo>(batchSize);

    for (HRegionInfo hri: regions) {
      HRegionInfo clonedHri = cloneRegion(hri);
      createdRegions.add(clonedHri);

      if (createdRegions.size() % batchSize == 0) {
        // Insert into META
        LOG.debug("add " + createdRegions.size() + " regions to .META.");
        MetaEditor.addRegionsToMeta(catalogTracker, createdRegions);
        createdRegions.clear();
      }
    }

    // Insert into META
    if (createdRegions.size() > 0) {
      LOG.debug("add " + createdRegions.size() + " regions to .META.");
      MetaEditor.addRegionsToMeta(catalogTracker, createdRegions);
    }
  }

  /**
   * Clone region from the snapshot info.
   *
   * Each region is encoded with the table name, so the cloned region will have
   * a different region name.
   *
   * Instead of copying the hfiles a HFileLink is created.
   *
   * @param snapshotRegionInfo
   * @return new region info
   */
  private HRegionInfo cloneRegion(final HRegionInfo snapshotRegionInfo) throws IOException {
    final Path rootDir = FSUtils.getRootDir(conf);
    final HRegion region = cloneRegion(snapshotRegionInfo, rootDir, conf, tableDesc);
    final HRegionInfo regionInfo = region.getRegionInfo();
    LOG.info("clone region=" + snapshotRegionInfo.getEncodedName() +
             " as " + regionInfo.getEncodedName());
    try {
      final Path regionDir = new Path(tableDir, regionInfo.getEncodedName());
      final String tableName = tableDesc.getNameAsString();
      SnapshotReferenceUtil.listStoreFiles(fs, regionDir,
        new FSVisitor.StoreFileVisitor() {
          public void storeFile (final String region, final String family, final String hfile)
              throws IOException {
            LOG.trace("Adding HFileLink " + hfile + " to table=" + tableName);
            Path familyDir = new Path(regionDir, family);
            HFileLink.create(conf, fs, familyDir, snapshotRegionInfo, hfile);
          }
      });
    } finally {
      region.close();
    }
    regionsMap.put(snapshotRegionInfo.getEncodedNameAsBytes(), regionInfo.getEncodedNameAsBytes());
    return regionInfo;
  }

  /**
   * Create a new {@link HRegion} from the snapshot region info.
   *
   * @param snapshotRegionInfo Info for region to clone.
   * @param rootDir Root directory for HBase instance
   * @param conf
   * @param tableDescriptor
   * @return the new HRegion instance
   * @throws IOException
   */
  public static HRegion cloneRegion(final HRegionInfo snapshotRegionInfo, final Path rootDir,
      final Configuration conf, final HTableDescriptor tableDescriptor) throws IOException {
    HRegionInfo regionInfo = new HRegionInfo(tableDescriptor.getName(),
                      snapshotRegionInfo.getStartKey(), snapshotRegionInfo.getEndKey(),
                      snapshotRegionInfo.isSplit(), snapshotRegionInfo.getRegionId());
    return HRegion.createHRegion(regionInfo, rootDir, conf, tableDescriptor, null, false, true);
  }

  /**
   * Restore snapshot WALs.
   *
   * Global Snapshot keep a reference to region servers logs present during the snapshot.
   *    /hbase/.snapshot/<name>/.logs/<host>/<name>
   *
   * Since each log contains different tables data, logs must be splitted extract table data.
   */
  private void restoreWALs() throws IOException {
    final SnapshotLogSplitter logSplitter = new SnapshotLogSplitter(conf, fs, tableDir,
                                Bytes.toBytes(snapshotDesc.getTable()), regionsMap);
    try {
      // Recover.Edits
      SnapshotReferenceUtil.listRecoveredEdits(fs, snapshotDir,
          new FSVisitor.RecoveredEditsVisitor() {
        public void recoveredEdits (final String region, final String logfile) throws IOException {
          Path path = SnapshotReferenceUtil.getRecoveredEdits(snapshotDir, region, logfile);
          logSplitter.splitRecoveredEdit(path);
        }
      });

      // Region Server Logs
      SnapshotReferenceUtil.listLogFiles(fs, snapshotDir, new FSVisitor.LogFileVisitor() {
        public void logFile (final String server, final String logfile) throws IOException {
          logSplitter.splitLog(server, logfile);
        }
      });
    } finally {
      logSplitter.close();
    }
  }

  /**
   * @return the set of the regions contained in the table
   */
  private List<HRegionInfo> getTableRegions() throws IOException {
    LOG.debug("get table regions: " + tableDir);
    FileStatus[] regionDirs = FSUtils.listStatus(fs, tableDir, new FSUtils.RegionDirFilter(fs));
    if (regionDirs == null) return null;

    List<HRegionInfo> regions = new LinkedList<HRegionInfo>();
    for (FileStatus regionDir: regionDirs) {
      HRegionInfo hri = HRegion.loadDotRegionInfoFileContent(fs, regionDir.getPath());
      regions.add(hri);
    }
    LOG.debug("found " + regions.size() + " regions for table=" + tableDesc.getNameAsString());
    return regions;
  }

  /**
   * @param conf {@link Configuration} from which to check for the timeout
   * @param defaultWaitTime Default amount of time to wait, if none is in the configuration
   * @return the max amount of time the master should wait for a restore to complete
   */
  public static long getMaxMasterTimeout(final Configuration conf, long defaultWaitTime) {
    return conf.getLong(MASTER_WAIT_TIME_RESTORE_SNAPSHOT, defaultWaitTime);
  }

  /**
   * Create a new table descriptor cloning the snapshot table schema.
   *
   * @param admin
   * @param snapshotTableDescriptor
   * @param tableName
   * @return cloned table descriptor
   * @throws IOException
   */
  public static HTableDescriptor cloneTableSchema(final HTableDescriptor snapshotTableDescriptor,
      final byte[] tableName) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    for (HColumnDescriptor hcd: snapshotTableDescriptor.getColumnFamilies()) {
      htd.addFamily(hcd);
    }
    return htd;
  }

  /**
   * Create a new table cloning the snapshot table schema.
   *
   * @param admin
   * @param snapshotTableDescriptor
   * @param tableName
   * @return cloned table descriptor
   * @throws IOException
   */
  public static HTableDescriptor cloneTableSchema(final HBaseAdmin admin,
      final HTableDescriptor snapshotTableDescriptor, final byte[] tableName) throws IOException {
    HTableDescriptor htd = cloneTableSchema(snapshotTableDescriptor, tableName);
    admin.createTable(htd);
    return htd;
  }

  /**
   * Restore snapshot table schema on specified table.
   *
   * @param admin
   * @param snapshotTableDescriptor
   * @param tableName
   * @return restored table descriptor
   * @throws IOException
   */
  public static HTableDescriptor restoreTableSchema(final HBaseAdmin admin,
      final HTableDescriptor snapshotTableDescriptor,
      final byte[] tableName) throws IOException {
    admin.modifyTable(tableName, snapshotTableDescriptor);
    return snapshotTableDescriptor;
  }
}
