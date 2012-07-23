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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.snapshot.RegionServerSnapshotHandler;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test create/using/deleting snapshots from the client
 * <p>
 * This is an end-to-end test for the snapshot utility
 */
@Category(MediumTests.class)
public class TestSnapshotFromClient {
  private static final Log LOG = LogFactory.getLog(TestSnapshotFromClient.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final int NUM_RS = 2;
  private static final String STRING_TABLE_NAME = "test";
  private static final byte[] TEST_FAM = Bytes.toBytes("fam");
  private static final byte[] TABLE_NAME = Bytes.toBytes(STRING_TABLE_NAME);

  /**
   * Setup the config for the cluster
   */
  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(NUM_RS);
  }

  private static void setupConf(Configuration conf) {
    // disable the ui
    conf.setInt("hbase.regionsever.info.port", -1);
    // change the flush size to a small amount, regulating number of store files
    conf.setInt("hbase.hregion.memstore.flush.size", 25000);
    // so make sure we get a compaction when doing a load, but keep around some
    // files in the store
    conf.setInt("hbase.hstore.compaction.min", 10);
    conf.setInt("hbase.hstore.compactionThreshold", 10);
    // block writes if we get to 12 store files
    conf.setInt("hbase.hstore.blockingStoreFiles", 12);
    // drop the number of attempts for the hbase admin
    conf.setInt("hbase.client.retries.number", 1);
    // set the number of threads to use for taking the snapshot
    conf.setInt(RegionServerSnapshotHandler.SNAPSHOT_REQUEST_THREADS, 4);
  }

  @Before
  public void setup() throws Exception {
    UTIL.createTable(TABLE_NAME, TEST_FAM);
  }

  @After
  public void tearDown() throws Exception {
    UTIL.deleteTable(TABLE_NAME);
    // and cleanup the archive directory
    try {
      UTIL.getTestFileSystem().delete(new Path(UTIL.getDefaultRootDirPath(), ".archive"), true);
    } catch (IOException e) {
      LOG.warn("Failure to delete archive directory", e);
    }
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    try {
      UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      LOG.warn("failure shutting down cluster", e);
    }
  }

  /**
   * Basic end-to-end test of globally-consistent snapshots
   * @throws Exception
   */
  @Test
  public void testGlobalCreateListDestroy() throws Exception {
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    // make sure we don't fail on listing snapshots
    assertEquals(0, admin.listSnapshots().length);
    // load the table so we have some data
    UTIL.loadTable(new HTable(UTIL.getConfiguration(), TABLE_NAME), TEST_FAM);
    // and wait until everything stabilizes
    HRegionServer rs = UTIL.getRSForFirstRegionInTable(TABLE_NAME);
    List<HRegion> onlineRegions = rs.getOnlineRegions(TABLE_NAME);
    for (HRegion region : onlineRegions) {
      region.waitForFlushesAndCompactions();
    }
    byte[] snapshotName = Bytes.toBytes("snapshot");
    // test creating the snapshot
    admin.globalSnapshot(snapshotName, TABLE_NAME);
    logFSTree(new Path(UTIL.getConfiguration().get(HConstants.HBASE_DIR)));

    // list the snapshot
    SnapshotDescriptor[] snapshots = admin.listSnapshots();

    assertEquals("Should only have 1 snapshot", 1, snapshots.length);
    assertArrayEquals(snapshotName, snapshots[0].getSnapshotName());
    assertArrayEquals(TABLE_NAME, snapshots[0].getTableName());

    // check the directory structure
    FileSystem fs = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getFileSystem();
    Path rootDir = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();
    SnapshotTestingUtils.confirmSnapshotValid(snapshots[0], TABLE_NAME, TEST_FAM, rootDir, admin,
      fs, true);

    // test that we can delete the snapshot
    admin.deleteSnapshot(snapshotName);

    // make sure we don't have any snapshots
    assertEquals("Snapshot wasn't deleted", 0, admin.listSnapshots().length);
  }

  /**
   * Test snapshotting a table that is offline
   * @throws Exception
   */
  @Test
  public void testOfflineTableSnapshot() throws Exception {
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    // make sure we don't fail on listing snapshots
    assertEquals("Have some previous snapshots", 0, admin.listSnapshots().length);

    // put some stuff in the table
    HTable table = new HTable(UTIL.getConfiguration(), TABLE_NAME);
    UTIL.loadTable(table, TEST_FAM);

    // disable the table
    admin.disableTable(TABLE_NAME);

    LOG.debug("FS state before snapshot:");
    FSUtils.logFileSystemState(UTIL.getTestFileSystem(),
      FSUtils.getRootDir(UTIL.getConfiguration()), LOG);

    // take a snapshot of the disabled table
    byte[] snapshot = Bytes.toBytes("snapshot");
    admin.snapshot(snapshot, TABLE_NAME);
    LOG.debug("Snapshot completed.");

    // make sure we have the snapshot
    SnapshotDescriptor[] snapshots = admin.listSnapshots();
    assertEquals("Snapshot wasn't taken.", 1, snapshots.length);

    // make sure its a valid snapshot
    FileSystem fs = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getFileSystem();
    Path rootDir = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();
    SnapshotTestingUtils.confirmSnapshotValid(snapshots[0], TABLE_NAME, TEST_FAM, rootDir, admin,
      fs, false);

    admin.deleteSnapshot(snapshot);
    snapshots = admin.listSnapshots();
    assertEquals("Snapshot wasn't deleted.", 0, snapshots.length);
  }

  /**
   * Basic end-to-end test of timestamp based snapshots
   * @throws Exception
   */
  @Test
  public void testTimestampedCreateListDestroy() throws Exception {
    LOG.debug("------- Starting Snapshot test -------------");
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    // make sure we don't fail on listing snapshots
    assertEquals(
      "There are existing snapshots - there shouldn't be. Its probably a broken test above.", 0,
      admin.listSnapshots().length);
    // load the table so we have some data
    UTIL.loadTable(new HTable(UTIL.getConfiguration(), TABLE_NAME), TEST_FAM);
    // and wait until everything stabilizes
    HRegionServer rs = UTIL.getRSForFirstRegionInTable(TABLE_NAME);
    List<HRegion> onlineRegions = rs.getOnlineRegions(TABLE_NAME);
    for (HRegion region : onlineRegions) {
      region.waitForFlushesAndCompactions();
    }
    byte[] snapshotName = Bytes.toBytes("timestampSnapshotCreateListDestroy");
    // test creating the snapshot
    admin.snapshot(snapshotName, TABLE_NAME);
    logFSTree(new Path(UTIL.getConfiguration().get(HConstants.HBASE_DIR)));

    // list the snapshot
    SnapshotDescriptor[] snapshots = admin.listSnapshots();

    assertEquals("Should only have 1 snapshot", 1, snapshots.length);
    assertArrayEquals(snapshotName, snapshots[0].getSnapshotName());
    assertArrayEquals(TABLE_NAME, snapshots[0].getTableName());

    // check the directory structure
    FileSystem fs = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getFileSystem();
    Path rootDir = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();
    Path snapshotDir = SnapshotDescriptor.getCompletedSnapshotDir(snapshots[0], rootDir);
    assertTrue(fs.exists(snapshotDir));
    Path snapshotinfo = new Path(snapshotDir, SnapshotDescriptor.SNAPSHOTINFO_FILE);
    assertTrue(fs.exists(snapshotinfo));

    // check the table info
    HTableDescriptor desc = FSTableDescriptors.getTableDescriptor(fs, rootDir, TABLE_NAME);
    HTableDescriptor snapshotDesc = FSTableDescriptors.getTableDescriptor(fs, snapshotDir,
      TABLE_NAME);
    assertEquals(desc, snapshotDesc);

    // check the region snapshot for all the regions
    List<HRegionInfo> regions = admin.getTableRegions(TABLE_NAME);
    for (HRegionInfo info : regions) {
      String regionName = info.getEncodedName();
      Path regionDir = new Path(snapshotDir, regionName);
      HRegionInfo snapshotRegionInfo = HRegion.loadDotRegionInfoFileContent(fs, regionDir);
      assertEquals(info, snapshotRegionInfo);
      // check to make sure we have the family
      Path familyDir = new Path(regionDir, Bytes.toString(TEST_FAM));
      assertTrue(fs.exists(familyDir));
      // make sure we have some file references
      assertTrue(fs.listStatus(familyDir).length > 0);
    }

    // test that we can delete the snapshot
    admin.deleteSnapshot(snapshotName);

    // make sure we don't have any snapshots
    assertEquals("Snapshot wasn't deleted", 0, admin.listSnapshots().length);
  }

  private void logFSTree(Path root) throws IOException {
    LOG.debug("Current file system:");
    logFSTree(root, "|-");
  }

  private void logFSTree(Path root, String prefix) throws IOException {
    for (FileStatus file : UTIL.getDFSCluster().getFileSystem().listStatus(root)) {
      if (file.isDir()) {
        LOG.debug(prefix + file.getPath().getName() + "/");
        logFSTree(file.getPath(), prefix + "---");
      } else {
        LOG.debug(prefix + file.getPath().getName());
      }
    }
  }
}