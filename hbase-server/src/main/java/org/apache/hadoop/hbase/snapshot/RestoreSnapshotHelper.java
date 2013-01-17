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

package org.apache.hadoop.hbase.snapshot;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.FSVisitor;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;

/**
 * Helper to Restore/Clone a Snapshot
 *
 * <p>The helper assumes that a table is already created, and by calling restore()
 * the content present in the snapshot will be restored as the new content of the table.
 *
 * <p>Clone from Snapshot: If the target table is empty, the restore operation
 * is just a "clone operation", where the only operations are:
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

  private final Map<byte[], byte[]> regionsMap =
        new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);

  private final ForeignExceptionDispatcher monitor;

  private final SnapshotDescription snapshotDesc;
  private final Path snapshotDir;

  private final HTableDescriptor tableDesc;
  private final Path tableDir;

  private final CatalogTracker catalogTracker;
  private final Configuration conf;
  private final FileSystem fs;

  private MonitoredTask status;

  public RestoreSnapshotHelper(final Configuration conf, final FileSystem fs,
      final CatalogTracker catalogTracker,
      final SnapshotDescription snapshotDescription, final Path snapshotDir,
      final HTableDescriptor tableDescriptor, final Path tableDir,
      final ForeignExceptionDispatcher monitor,final MonitoredTask status)
  {
    this.fs = fs;
    this.conf = conf;
    this.catalogTracker = catalogTracker;
    this.snapshotDesc = snapshotDescription;
    this.snapshotDir = snapshotDir;
    this.tableDesc = tableDescriptor;
    this.tableDir = tableDir;
    this.monitor = monitor;
    this.status = status;
  }

  /**
   * Restore table to a specified snapshot state.
   */
  public void restore() throws IOException {
    long startTime = EnvironmentEdgeManager.currentTimeMillis();

    LOG.debug("starting restore");
    Set<String> snapshotRegionNames = SnapshotReferenceUtil.getSnapshotRegionNames(fs, snapshotDir);
    if (snapshotRegionNames == null) {
      LOG.warn("Nothing to restore. Snapshot " + snapshotDesc + " looks empty");
      return;
    }

    // Identify which region are still available and which not.
    // NOTE: we rely upon the region name as: "table name, start key, end key"
    List<HRegionInfo> tableRegions = getTableRegions();
    if (tableRegions != null) {
      monitor.rethrowException();
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
      monitor.rethrowException();
      status.setStatus("Restoring table regions...");
      restoreRegions(regionsToRestore);
      status.setStatus("Finished restoring all table regions.");

      // Remove regions from the current table
      monitor.rethrowException();
      status.setStatus("Starting to delete excess regions from table");
      ModifyRegionUtils.deleteRegions(fs, catalogTracker, regionsToRemove);
    }

    // Regions to Add: present in the snapshot but not in the current table
    if (snapshotRegionNames.size() > 0) {
      List<HRegionInfo> regionsToAdd = new LinkedList<HRegionInfo>();

      monitor.rethrowException();
      for (String regionName: snapshotRegionNames) {
        LOG.info("region to add: " + regionName);
        Path regionDir = new Path(snapshotDir, regionName);
        regionsToAdd.add(HRegion.loadDotRegionInfoFileContent(fs, regionDir));
      }

      // Create new regions cloning from the snapshot
      monitor.rethrowException();
      status.setStatus("Cloning regions...");
      cloneRegions(regionsToAdd);
      status.setStatus("Finished cloning regions.");
    }

    // Restore WALs
    monitor.rethrowException();
    status.setStatus("Restoring WALs to table...");
    restoreWALs();
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
        restoreStoreFile(familyDir, regionInfo, hfileName);
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
      familyFiles.add(hfileName);
    }

    return familyFiles;
  }

  /**
   * Clone specified regions. For each region create a new region
   * and create a HFileLink for each hfile.
   */
  private void cloneRegions(final List<HRegionInfo> regions) throws IOException {
    if (regions == null || regions.size() == 0) return;

    final Map<String, HRegionInfo> snapshotRegions =
      new HashMap<String, HRegionInfo>(regions.size());

    // clone region info (change embedded tableName with the new one)
    HRegionInfo[] clonedRegionsInfo = new HRegionInfo[regions.size()];
    for (int i = 0; i < clonedRegionsInfo.length; ++i) {
      // clone the region info from the snapshot region info
      HRegionInfo snapshotRegionInfo = regions.get(i);
      clonedRegionsInfo[i] = cloneRegionInfo(snapshotRegionInfo);

      // add the region name mapping between snapshot and cloned
      String snapshotRegionName = snapshotRegionInfo.getEncodedName();
      String clonedRegionName = clonedRegionsInfo[i].getEncodedName();
      regionsMap.put(Bytes.toBytes(snapshotRegionName), Bytes.toBytes(clonedRegionName));
      LOG.info("clone region=" + snapshotRegionName + " as " + clonedRegionName);

      // Add mapping between cloned region name and snapshot region info
      snapshotRegions.put(clonedRegionName, snapshotRegionInfo);
    }

    // create the regions on disk
    List<HRegionInfo> clonedRegions = ModifyRegionUtils.createRegions(conf, FSUtils.getRootDir(conf),
      tableDesc, clonedRegionsInfo, catalogTracker, new ModifyRegionUtils.RegionFillTask() {
        public void fillRegion(final HRegion region) throws IOException {
          cloneRegion(region, snapshotRegions.get(region.getRegionInfo().getEncodedName()));
        }
      });
    if (regions != null && regions.size() > 0) {
      // add regions to .META.
      MetaEditor.addRegionsToMeta(catalogTracker, clonedRegions);
    }
  }

  /**
   * Clone region directory content from the snapshot info.
   *
   * Each region is encoded with the table name, so the cloned region will have
   * a different region name.
   *
   * Instead of copying the hfiles a HFileLink is created.
   *
   * @param region {@link HRegion} cloned
   * @param snapshotRegionInfo
   */
  private void cloneRegion(final HRegion region, final HRegionInfo snapshotRegionInfo)
      throws IOException {
    final Path snapshotRegionDir = new Path(snapshotDir, snapshotRegionInfo.getEncodedName());
    final Path regionDir = new Path(tableDir, region.getRegionInfo().getEncodedName());
    final String tableName = tableDesc.getNameAsString();
    SnapshotReferenceUtil.visitRegionStoreFiles(fs, snapshotRegionDir,
      new FSVisitor.StoreFileVisitor() {
        public void storeFile (final String region, final String family, final String hfile)
            throws IOException {
          LOG.info("Adding HFileLink " + hfile + " to table=" + tableName);
          Path familyDir = new Path(regionDir, family);
          restoreStoreFile(familyDir, snapshotRegionInfo, hfile);
        }
    });
  }

  /**
   * Create a new {@link HFileLink} to reference the store file.
   *
   * @param familyDir destination directory for the store file
   * @param regionInfo destination region info for the table
   * @param hfileName store file name (can be a Reference, HFileLink or simple HFile)
   */
  private void restoreStoreFile(final Path familyDir, final HRegionInfo regionInfo,
      final String hfileName) throws IOException {
    if (HFileLink.isHFileLink(hfileName)) {
      HFileLink.createFromHFileLink(conf, fs, familyDir, hfileName);
    } else {
      HFileLink.create(conf, fs, familyDir, regionInfo, hfileName);
    }
  }

  /**
   * Create a new {@link HRegionInfo} from the snapshot region info.
   * Keep the same startKey, endKey, regionId and split information but change
   * the table name.
   *
   * @param snapshotRegionInfo Info for region to clone.
   * @return the new HRegion instance
   */
  public HRegionInfo cloneRegionInfo(final HRegionInfo snapshotRegionInfo) {
    return new HRegionInfo(tableDesc.getName(),
                      snapshotRegionInfo.getStartKey(), snapshotRegionInfo.getEndKey(),
                      snapshotRegionInfo.isSplit(), snapshotRegionInfo.getRegionId());
  }

  /**
   * Restore snapshot WALs.
   *
   * Global Snapshot keep a reference to region servers logs present during the snapshot.
   * (/hbase/.snapshot/snapshotName/.logs/hostName/logName)
   *
   * Since each log contains different tables data, logs must be split to
   * extract the table that we are interested in.
   */
  private void restoreWALs() throws IOException {
    final SnapshotLogSplitter logSplitter = new SnapshotLogSplitter(conf, fs, tableDir,
                                Bytes.toBytes(snapshotDesc.getTable()), regionsMap);
    try {
      // Recover.Edits
      SnapshotReferenceUtil.visitRecoveredEdits(fs, snapshotDir,
          new FSVisitor.RecoveredEditsVisitor() {
        public void recoveredEdits (final String region, final String logfile) throws IOException {
          Path path = SnapshotReferenceUtil.getRecoveredEdits(snapshotDir, region, logfile);
          logSplitter.splitRecoveredEdit(path);
        }
      });

      // Region Server Logs
      SnapshotReferenceUtil.visitLogFiles(fs, snapshotDir, new FSVisitor.LogFileVisitor() {
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
}
