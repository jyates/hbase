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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HFileArchiveManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.zookeeper.HFileArchiveTracker;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

/**
 * Spin up a small cluster and check that a region properly archives its hfiles
 * when enabled.
 */
@Category(MediumTests.class)
public class TestRegionHFileArchiving {

  private static final Log LOG = LogFactory.getLog(TestRegionHFileArchiving.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final String STRING_TABLE_NAME = "test";
  private static final byte[] TEST_FAM = Bytes.toBytes("fam");
  private static final byte[] TABLE_NAME = Bytes.toBytes(STRING_TABLE_NAME);
  private static final int numRS = 2;
  private static final int maxTries = 10;

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
    // each loadRegion is 421,000 bytes, meaning we get 16 store files
    conf.setInt("hbase.hregion.memstore.flush.size", 25000);
    // so make sure we get a compaction when doing a load, but keep around some
    // files in the store
    conf.setInt("hbase.hstore.compaction.min", 10);
    conf.setInt("hbase.hstore.compactionThreshold", 10);
    // block writes if we get to 12 store files (ensures we get compactions at
    // the expected times)
    conf.setInt("hbase.hstore.blockingStoreFiles", 12);
    // check memstore size frequently (100ms)
    // conf.setInt("hbase.server.thread.wakefrequency", 100);
    // drop the number of attempts for the hbase admin
    UTIL.getConfiguration().setInt("hbase.client.retries.number", 5);
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

  @AfterClass
  public static void cleanupTest() throws Exception {
    try {
      UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      // NOOP;
    }
  }

  @Test
  public void testEnableDisableArchiving() throws Exception {
    // Make sure archiving is not enabled
    ZooKeeperWatcher zk = UTIL.getZooKeeperWatcher();
    assertFalse(getArchivingEnabled(zk, TABLE_NAME));

    // get the RS and region serving our table
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    List<HRegion> servingRegions = UTIL.getHBaseCluster().getRegions(TABLE_NAME);
    // make sure we only have 1 region serving this table
    assertEquals(1, servingRegions.size());
    HRegion region = servingRegions.get(0);
    HRegionServer hrs = UTIL.getRSForFirstRegionInTable(TABLE_NAME);
    FileSystem fs = hrs.getFileSystem();

    // enable archiving
    admin.enableHFileBackup(TABLE_NAME, Bytes.toBytes(".archive"));
    assertTrue(getArchivingEnabled(zk, TABLE_NAME));

    // put some load on the table and make sure that files do get archived
    loadAndCompact(region);

    // check that we actually have some store files
    // make sure we don't have any extra archive files from random compactions
    HFileArchiveMonitor monitor = hrs.getHFileArchiveMonitor();
    Store store = region.getStore(TEST_FAM);
    Path storeArchiveDir = HFileArchiveUtil.getStoreArchivePath(monitor, region.getTableDir(),
      region.getRegionInfo().getEncodedName(), store.getFamily().getName());
    assertTrue(fs.exists(storeArchiveDir));
    assertTrue(fs.listStatus(storeArchiveDir).length > 0);

    // now test that we properly stop backing up
    LOG.debug("Stopping backup and testing that we don't archive");
    admin.disableHFileBackup(STRING_TABLE_NAME);
    int counter = 0;
    while (monitor.keepHFiles(STRING_TABLE_NAME)) {
      LOG.debug("Waiting for archive change to propaate");
      Thread.sleep(500);
      // max of 10 tries to propagate - if not, something is probably horribly
      // wrong with zk/notification
      assertTrue(counter++ < maxTries);
    }

    // delete the existing archive files
    fs.delete(storeArchiveDir, true);

    // and then put some more data in the table and ensure it compacts
    loadAndCompact(region);

    // put data into the table again to make sure that we don't copy new files
    // over into the archive directory

    // ensure there are no archived files
    assertFalse(fs.exists(storeArchiveDir));

    // make sure that overall backup also disables per-table
    admin.enableHFileBackup(TABLE_NAME, Bytes.toBytes(".archive"));
    admin.disableHFileBackup();
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
   * Ensure that archiving won't be turned on but have the tracker miss the
   * update to the table via incorrect ZK watches (especially because
   * createWithParents is not transactional).
   * @throws Exception
   */
  @Test
  public void testFindsTablesAfterArchivingEnabled() throws Exception {
    // 1. create a tracker to track the nodes
    ZooKeeperWatcher zkw = UTIL.getZooKeeperWatcher();
    HRegionServer hrs = UTIL.getRSForFirstRegionInTable(TABLE_NAME);
    HFileArchiveTracker tracker = hrs.hfileArchiveTracker;

    // 2. create the archiving enabled znode
    ZKUtil.createAndFailSilent(zkw, zkw.archiveHFileZNode);

    // 3. now turn on archiving for the test table
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    admin.enableHFileBackup(TABLE_NAME);

    // 4. make sure that archiving is enabled for that tracker
    assertEquals(".archive", tracker.getBackupDirectory(STRING_TABLE_NAME));
  }

  /**
   * Test that we do synchronously start archiving and not return until we are
   * done
   */
  @Test
  public void testSynchronousArchiving() throws Exception {
    LOG.debug("****** Starting synchronous archiving test");
    HBaseAdmin admin = UTIL.getHBaseAdmin();

    ZooKeeperWatcher zk = UTIL.getZooKeeperWatcher();

    // 1. turn on hfile backups
    LOG.debug("----Starting archiving");
    admin.enableHFileBackup(TABLE_NAME);
    assertTrue(getArchivingEnabled(zk, TABLE_NAME));

    // 2. ensure that backups are kept on each RS
    // get all the monitors
    for (int i = 0; i < numRS; i++) {
      HRegionServer hrs = UTIL.getHBaseCluster().getRegionServer(i);
      // make sure that at least regions hosting the table have received the
      // update to start archiving
      if (hrs.getOnlineRegions(TABLE_NAME).size() > 0) {
        assertTrue(hrs.getHFileArchiveMonitor().keepHFiles(STRING_TABLE_NAME));
      }
    }

    // 3. now attempt to archive some other table that doesn't exist
    try {
      admin.enableHFileBackup("other table");
      fail("Should get an IOException if a table cannot be backed up.");
    } catch (IOException e) {
      // this should happen
    }
    assertFalse("Table 'other table' should not be archived - it doesn't exist!",
      getArchivingEnabled(zk, Bytes.toBytes("other table")));
  }

  /**
   * Test the advanced case where we turn on archiving and the region propagates
   * the change down to the store
   */
  // @Ignore("Testing for borked test")
  @Test
  public void testCompactAndArchive() throws Exception {
    HBaseAdmin admin = UTIL.getHBaseAdmin();

    // get the RS and region serving our table
    List<HRegion> servingRegions = UTIL.getHBaseCluster().getRegions(TABLE_NAME);
    // make sure we only have 1 region serving this table
    assertEquals(1, servingRegions.size());
    HRegion region = servingRegions.get(0);

    // get the parent RS and monitor
    HRegionServer hrs = UTIL.getRSForFirstRegionInTable(TABLE_NAME);
    FileSystem fs = hrs.getFileSystem();
    HFileArchiveMonitor monitor = hrs.getHFileArchiveMonitor();
    Store store = region.getStores().get(TEST_FAM);

    // 1. put some data on the region
    LOG.debug("-------Loading table");
    UTIL.loadRegion(region, TEST_FAM);

    // get the current store files for the region
    // and that there is only one store in the region
    assertEquals(1, region.getStores().size());

    int fileCount = store.getStorefiles().size();
    assertTrue("Need more than 1 store file to compact and test archiving", fileCount > 1);
    LOG.debug("Currently have: " + fileCount + " store files.");
    LOG.debug("Has store files:");
    for (StoreFile sf : store.getStorefiles()) {
      LOG.debug("\t" + sf.getPath());
    }

    // 2. make sure that table archiving is enabled
    // first force a flush to make sure nothing weird happens
    region.flushcache();

    // turn on hfile backups into .archive
    LOG.debug("-----Enabling backups");
    admin.enableHFileBackup(TABLE_NAME, Bytes.toBytes(".archive"));

    // make sure we don't have any extra archive files from random compactions
    Path storeArchiveDir = HFileArchiveUtil.getStoreArchivePath(monitor, region.getTableDir(),
      region.getRegionInfo().getEncodedName(), store.getFamily().getName());
    LOG.debug("-----Initial files in store archive:");
    if (fs.exists(storeArchiveDir)) {
      for (FileStatus f : fs.listStatus(storeArchiveDir)) {
        LOG.debug("Deleting archive file: " + f.getPath()
            + ", so we have a consistent backup view.");
      }
      fs.delete(storeArchiveDir, true);
    } else LOG.debug("[EMPTY]");

    // make sure that archiving is in a 'clean' state
    assertNull(fs.listStatus(storeArchiveDir));

    // make sure we block the store from compacting/flushing files in the middle
    // of our
    // copy of the store files
    List<StoreFile> origFiles;
    FileStatus[] originals;
    List<Path> copiedStores;
    synchronized (store.filesCompacting) {
      synchronized (store.flushLock) {
        LOG.debug("Locked the store");
        // get the original store files before compaction
        LOG.debug("------Original store files:");
        originals = fs.listStatus(store.getHomedir());
        for (FileStatus f : originals) {
          LOG.debug("\t" + f.getPath());
        }
        // copy the original store files so we can use them for testing
        // overwriting
        // store files with the same name below
        origFiles = store.getStorefiles();
        copiedStores = new ArrayList<Path>(origFiles.size());
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
      }
    }// finish being synchronized, so we can do a compaction
    LOG.debug("Unlocked the store");
    // and then trigger a compaction to combine the files again
    LOG.debug("---------- Triggering compaction");
    compactRegion(region, TEST_FAM);

    // then get the archived store files
    LOG.debug("----------Archived store files after compaction:");
    FileStatus[] archivedFiles = fs.listStatus(storeArchiveDir);
    for (FileStatus f : archivedFiles) {
      LOG.debug("\t" + f.getPath());
    }

    // ensure the archived files match the original store files (at least in
    // naming)
    compareArchiveToOriginal(originals, archivedFiles, fs);
    LOG.debug("Archive matches originals.");

    // 3. Now copy back in the store files and trigger another compaction

    // first delete out the existing files
    LOG.debug("Deleting out existing store files, and moving in our copies.");

    // lock again so we don't interfere with a compaction/flush
    synchronized (store.filesCompacting) {
      synchronized (store.flushLock) {
        LOG.debug("Locked the store");
        // delete the store directory (just in case)
        fs.delete(store.getHomedir(), true);

        // and copy back in the original store files (from before)
        fs.mkdirs(store.getHomedir());
        for (int i = 0; i < copiedStores.size(); i++) {
          fs.rename(copiedStores.get(i), origFiles.get(i).getPath());
        }

        // now archive the files again
        LOG.debug("Removing the store files again.");
        store.removeStoreFiles(hrs.getHFileArchiveMonitor(), origFiles);

        // ensure the files match to originals, but with a backup directory
        LOG.debug("Checking originals vs. backed up (from archived) versions");
        archivedFiles = fs.listStatus(storeArchiveDir);
        compareArchiveToOriginal(originals, archivedFiles, fs, true);

      }
    }
    LOG.debug("Unlocked the store");
  }

  // @Ignore("Testing for borked test")
  @Test
  public void testRegionSplitAndArchive() throws Exception {
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
    UTIL.loadRegion(region, TEST_FAM);

    // get the files before compaction
    FileSystem fs = region.getRegionServerServices().getFileSystem();
    FileStatus[] originals = fs.listStatus(store.getHomedir());

    //delete out the current archive files, just for ease of comparison
    // and synchronize to make sure we don't clobber another compaction
    HFileArchiveMonitor monitor = hrs.getHFileArchiveMonitor();
    Path storeArchiveDir = HFileArchiveUtil.getStoreArchivePath(monitor, region.getTableDir(),
      region.getRegionInfo().getEncodedName(), store.getFamily().getName());

    synchronized (region.writestate) {
      // wait for all the compactions/flushes to complete on the region
      LOG.debug("Waiting on region " + region + "to complete compactions & flushes");
      region.waitForFlushesAndCompactions();
      LOG.debug("Removing archived files - general cleanup");
      assertTrue(fs.delete(storeArchiveDir, true));
      assertTrue(fs.mkdirs(storeArchiveDir));
    }
    LOG.debug("Starting split of region");
    // now split our region
    admin.split(TABLE_NAME);
    while (UTIL.getHBaseCluster().getRegions(TABLE_NAME).size() < 2) {
      LOG.debug("Waiting for regions to split.");
      Thread.sleep(100);
    }
    LOG.debug("Regions finished splitting.");
    // at this point the region should have split
    servingRegions = UTIL.getHBaseCluster().getRegions(TABLE_NAME);
    // make sure we now have 2 regions serving this table
    assertEquals(2, servingRegions.size());

    // now check to make sure that those daughter regions are part of the list
    // of those regions archiving a table
    HFileArchiveManager manager = new HFileArchiveManager(UTIL.getZooKeeperWatcher());
    List<String> regions = manager.regionServersArchiving(TABLE_NAME);
    for (HRegion r : servingRegions) {
      if (!regions.contains(r.getRegionInfo().getEncodedName())) {
        fail("Regions being archived doesn't include the daughter region:" + r);
      }
    }
  }

  /**
   * Load the given region and then ensure that it compacts some files
   */
  private void loadAndCompact(HRegion region) throws Exception {
    int tries = 0;
    Exception last = null;
    while (tries++ <= maxTries) {
      try {
        // load the region with data
        UTIL.loadRegion(region, TEST_FAM);
        // and then trigger a compaction to be sure we try to archive
        compactRegion(region, TEST_FAM);
        return;
      } catch (Exception e) {
        // keep this around for if we fail later
        last = e;
      }
    }
    if (last != null) throw last;

  }

  /**
   * Compact all the store files in a given region.
   */
  private void compactRegion(HRegion region, byte[] family) throws IOException {
    Store store = region.getStores().get(TEST_FAM);
    store.compactRecentForTesting(store.getStorefiles().size());
  }
}
