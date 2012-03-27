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
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.util.HFileArchiveTestingUtil.compareArchiveToOriginal;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HFileArchiveManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Spin up a small cluster and check that a region properly archives its hfiles
 * when enabled.
 */
@Category(MediumTests.class)
public class TestRegionHFileArchiving {

  private static final Log LOG = LogFactory.getLog(TestRegionHFileArchiving.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  HTable table;
  HTableDescriptor desc;
  private static final String STRING_TABLE_NAME = "test";
  private static final byte[] TEST_FAM = Bytes.toBytes("fam");
  private static final byte[] TEST_QUAL = Bytes.toBytes("qual");
  private static final byte[] TABLE_NAME = Bytes.toBytes(STRING_TABLE_NAME);
  private static final int numRS = 2;

  /**
   * Setup the config for the cluster
   */
  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(numRS);
  }

  private static void setupConf(Configuration conf) {
    // disable the ui
    conf.setInt("hbase.regionsever.info.port", -1);
    // set client side buffer to be 2000 bytes
    // conf.setInt("hbase.client.write.buffer", 2000);
    // set the memstore flush size to 2000 bytes
    conf.setInt("hbase.hregion.memstore.flush.size", 50000);
    // check memstore size frequently (100ms)
    // conf.setInt("hbase.server.thread.wakefrequency", 100);
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    try {
      UTIL.isRunningCluster();
    } catch (IOException e) {
      UTIL.shutdownMiniCluster();
    }
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
    // make sure that backups are off for all tables
    UTIL.getHBaseAdmin().disableHFileBackup();
  }

  @Test
  public void testSimpleEnableDisableArchiving() throws Exception {
    // 0. Make sure archiving is not enabled
    ZooKeeperWatcher zk = UTIL.getZooKeeperWatcher();
    assertFalse(getArchivingEnabled(zk, TABLE_NAME));

    HFileArchiveManager manager = new HFileArchiveManager(zk);
    manager.enableHFileBackup(TABLE_NAME, Bytes.toBytes(".archive"));

    assertTrue(getArchivingEnabled(zk, TABLE_NAME));

    manager.disableHFileBackup(TABLE_NAME);

    assertFalse(
      "Not empty, children of archive:"
          + org.apache.zookeeper.ZKUtil.listSubTreeBFS(zk.getRecoverableZooKeeper().getZooKeeper(),
            zk.archiveHFileZNode), getArchivingEnabled(zk, TABLE_NAME));

    // 0.a make sure that overally backup also disables per-table
    manager.enableHFileBackup(TABLE_NAME, Bytes.toBytes(".archive"));
    manager.disableHFileBackup(5);
    assertFalse(getArchivingEnabled(zk, TABLE_NAME));
  }

  /**
   * Check to see if archiving is enabled for a given table
   * @param zooKeeper watcher for the zk cluster
   * @param table name of the table to check
   * @return <tt>true</tt> if the table is being archived, <tt>false</tt>
   *         otherwise
   * @throws KeeperException if an unexpected ZK connection issues occurs
   */
  private static boolean getArchivingEnabled(ZooKeeperWatcher zooKeeper, byte[] table)
      throws KeeperException {
    // if not enabled for any table, then definitely not for this table
    if (ZKUtil.checkExists(zooKeeper, zooKeeper.archiveHFileZNode) < 0) return false;

    // build the table znode
    String tableNode = HFileArchiveUtil.getTableNode(zooKeeper, table);
    return ZKUtil.checkExists(zooKeeper, tableNode) >= 0;
  }

  /**
   * Test that we do synchronously start archiving and not return until we are
   * done
   */
  @Test
  public void testSynchronousArchiving() throws Exception {
    LOG.debug("****** Starting synchronous archiving test");
    HBaseAdmin admin = UTIL.getHBaseAdmin();

    // 1. Get all the monitors for the table
    List<HFileArchiveMonitor> monitors = new ArrayList<HFileArchiveMonitor>(numRS);
    // get all the monitors
    for (int i = 0; i < numRS; i++) {
      HRegionServer hrs = UTIL.getHBaseCluster().getRegionServer(i);
      monitors.add(hrs.getHFileArchiveMonitor());
    }
    ZooKeeperWatcher zk = UTIL.getZooKeeperWatcher();

    // 2. turn on hfile backups
    LOG.debug("----Starting archiving");
    try {
      admin.enableHFileBackup(TABLE_NAME);
      assertTrue(getArchivingEnabled(zk, TABLE_NAME));
    } catch (IOException e) {
      fail("Could not complete backup synchronously!");
      throw e;
    }
    // 3. ensure that backups are kept on each RS
    for (HFileArchiveMonitor montior : monitors) {
      assertTrue(montior.keepHFiles(STRING_TABLE_NAME));
    }

    // 4. now attempt to archive some other table
    admin.enableHFileBackup("other table");
    assertTrue(getArchivingEnabled(zk, Bytes.toBytes("other table")));
  }

  /**
   * Test the advanced case where we turn on archiving and the region propagates
   * the change down to the store
   */
  @Ignore("Testing for borked test")
  @Test
  public void testCompactAndArchive() throws Exception {
    HBaseAdmin admin = UTIL.getHBaseAdmin();

    // get the RS and region serving our table
    List<HRegion> servingRegions = UTIL.getHBaseCluster().getRegions(TABLE_NAME);
    // make sure we only have 1 region serving this table
    assertEquals(1, servingRegions.size());
    HRegion region = servingRegions.get(0);

    // get the parent RS
    HRegionServer hrs = UTIL.getRSForFirstRegionInTable(TABLE_NAME);
    FileSystem fs = hrs.getFileSystem();

    HFileArchiveMonitor monitor = hrs.getHFileArchiveMonitor();
    Store store = region.getStores().get(TEST_FAM);
    Path storeArchiveDir = HFileArchiveUtil.getStoreArchivePath(monitor, region.getTableDir(),
      region.getRegionInfo().getEncodedName(), store.getFamily().getName());
    LOG.debug("-----Initial files in store archive:");
    if (fs.exists(storeArchiveDir)) {
      for (FileStatus f : fs.listStatus(storeArchiveDir)) {
        LOG.debug("\t" + f.getPath());
      }
    }

    // 0. put some data on the region
    LOG.debug("-------Loading table");
    UTIL.loadRegion(region, TEST_FAM);

    // 1. make sure that table archiving is enabled
    // turn on hfile backups into .archive
    LOG.debug("-----Enabling backups");
    admin.enableHFileBackupAsync(TABLE_NAME, Bytes.toBytes(".archive"));

    // wait loop to make sure the change propagates
    while (!monitor.keepHFiles(STRING_TABLE_NAME)) {
      LOG.debug("Waiting for archive-enabled change to propaate.");
      Thread.sleep(100);
    } // get the current store files for the region
    // and that there is only one store in the region
    assertEquals(1, region.getStores().size());

    int fileCount = store.getStorefiles().size();
    assertTrue("Need more than 1 store file to compact and test archiving", fileCount > 1);

    LOG.debug("-----Files in store archive:");
    if (fs.exists(storeArchiveDir)) {
    for (FileStatus f : fs.listStatus(storeArchiveDir)) {
      LOG.debug("\t" + f.getPath());
    }
    } else LOG.debug("[EMPTY]");

    // get the files before compaction
    FileStatus[] originals = fs.listStatus(store.getHomedir());

    LOG.debug("------Original store files:");
    for (FileStatus f : originals) {
      LOG.debug("\t" + f.getPath());
    }

    // copy the original store files so we can use them for testing overwriting
    // store files with the same name below
    List<StoreFile> origFiles = store.getStorefiles();
    List<Path> copiedStores = new ArrayList<Path>(origFiles.size());
    Path temproot = new Path(hrs.getRootDir(), "store_copy");
    for (StoreFile f : origFiles) {
      if (!fs.exists(f.getPath())) continue;

      Path tmpStore = new Path(temproot, f.getPath().getName());
      FSDataOutputStream tmpOutput = fs.create(tmpStore);
      FSDataInputStream storeInput = fs.open(f.getPath());
      while (storeInput.available() > 0) {
        byte[] remaining = new byte[1024];
        storeInput.read(remaining);
        tmpOutput.write(remaining);
      }
      tmpOutput.close();
      storeInput.close();
      copiedStores.add(tmpStore);
    }

    LOG.debug("---------- Triggering compaction");
    // and then trigger a compaction
    compactRegion(region, TEST_FAM);

    // the get the store files in the archive
    LOG.debug("----------Store files after compaction:");
    for (FileStatus f : fs.listStatus(storeArchiveDir)) {
      LOG.debug("\t" + f.getPath());
    }

    FileStatus[] archivedFiles = fs.listStatus(storeArchiveDir);
    // ensure the files match to originals
    compareArchiveToOriginal(originals, archivedFiles, fs);

    // 2. Now copy back in the store files and trigger another compaction
    // first delete out the existing files
    fs.delete(store.getHomedir(), true);
    // and copy back in the existing files
    fs.mkdirs(store.getHomedir());
    for (int i = 0; i < copiedStores.size(); i++) {
      fs.rename(copiedStores.get(i), origFiles.get(i).getPath());
    }

    // now archive the files again
    store.removeStoreFiles(hrs.getHFileArchiveMonitor(), origFiles);

    // ensure the files match to originals, but with a backup directory
    archivedFiles = fs.listStatus(storeArchiveDir);
    compareArchiveToOriginal(originals, archivedFiles, fs, true);

    // 3. now test that we properly stop backing up
    // clean out the existing backup
    Path tableArchive = HFileArchiveUtil.getTableArchivePath(monitor, region.tableDir);
    assertTrue(fs.delete(tableArchive, true));

    // make sure that we don't copy new files over
    UTIL.loadRegion(region, TEST_FAM);

    // now disable the archiving
    admin.disableHFileBackup();
    while (monitor.keepHFiles(STRING_TABLE_NAME)) {
      LOG.debug("Waiting for archive change to propaate");
      Thread.sleep(500);
      // do a re-read of zk to ensure the change propagated
      hrs.hfileArchiveTracker.start();
    }

    // and then trigger a compaction
    compactRegion(region, TEST_FAM);

    // ensure there are no archived files
    assertNull(fs.listStatus(storeArchiveDir));
  }

  @Ignore("Testing for borked test")
  @Test
  public void testRegionSplitAndArchive() throws Exception {
    int splitRegionCount = 2; // total number of RS (post-split meta and root)
    HBaseAdmin admin = UTIL.getHBaseAdmin();

    // start archiving
    admin.enableHFileBackup(TABLE_NAME);

    // get the current store files for the region
    List<HRegion> servingRegions = UTIL.getHBaseCluster().getRegions(TABLE_NAME);
    // make sure we only have 1 region serving this table
    assertEquals(1, servingRegions.size());
    HRegion region = servingRegions.get(0);

    // get the parent RS
    HRegionServer hrs = UTIL.getRSForFirstRegionInTable(TABLE_NAME);
    // UTIL.getUTIL.getHBaseCluster().getServerWith(region.getRegionName());

    // and that there is only one store in the region
    assertEquals(1, region.getStores().size());
    Store store = region.getStores().get(TEST_FAM);

    // prep the store files so we get some files
    LOG.debug("Loading store files");
    // prepStoreFiles(admin, store, 3);
    UTIL.loadTable(new HTable(UTIL.getConfiguration(), TABLE_NAME), TEST_FAM);

    // get the files before compaction
    FileSystem fs = region.getRegionServerServices().getFileSystem();
    FileStatus[] originals = fs.listStatus(store.getHomedir());

    LOG.debug("Starting split of region");
    // now split our region
    admin.split(region.getRegionNameAsString());

    while (UTIL.getHBaseCluster().getRegions(TABLE_NAME).size() < splitRegionCount) {
      LOG.debug("Waiting on region to split");
      Thread.sleep(100);
    }
    // at this point the region should have split
    servingRegions = UTIL.getHBaseCluster().getRegions(TABLE_NAME);
    // make sure we now have 2 regions serving this table (and 2 meta regions)
    assertEquals(splitRegionCount, servingRegions.size());

    // and then force a compaction of the regions and read out the new files
    LOG.debug("Compacting all the stores for testing");
    HFileArchiveMonitor monitor = hrs.getHFileArchiveMonitor();
    List<FileStatus> archived = new ArrayList<FileStatus>();
    for (HRegion r : hrs.getOnlineRegions(TABLE_NAME)) {
      // do compaction
      compactRegion(r, TEST_FAM);

      // get the new files
      Store s = r.getStore(TEST_FAM);
      Path storeArchiveDir = HFileArchiveUtil.getStoreArchivePath(monitor, r.getTableDir(), r
          .getRegionInfo().getEncodedName(), s.getFamily().getName());
      for (FileStatus file : fs.listStatus(storeArchiveDir)) {
        archived.add(file);
      }
    }

    // and check the archive files
    compareArchiveToOriginal(originals, archived.toArray(new FileStatus[0]), fs);
  }

  private void compactRegion(HRegion region, byte[] family) throws IOException {
    Store store = region.getStores().get(TEST_FAM);
    store.compactRecentForTesting(store.getStorefiles().size());
  }
}
