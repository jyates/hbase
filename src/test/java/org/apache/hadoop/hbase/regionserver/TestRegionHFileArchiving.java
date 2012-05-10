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

import static org.apache.hadoop.hbase.util.HFileArchiveTestingUtil.assertArchiveEqualToOriginal;
import static org.apache.hadoop.hbase.util.HFileArchiveTestingUtil.compareArchiveToOriginal;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
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
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.backup.HFileArchiveMonitor;
import org.apache.hadoop.hbase.backup.HFileArchiveTableMonitor;
import org.apache.hadoop.hbase.backup.RegionDisposer;
import org.apache.hadoop.hbase.backup.TableHFileArchiveTracker;
import org.apache.hadoop.hbase.backup.SimpleHFileArchiveTableMonitor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HFileArchiveTestingUtil;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;

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
  private static final int maxTries = 5;

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
    // change the flush size to a small amount, regulating number of store files
    conf.setInt("hbase.hregion.memstore.flush.size", 25000);
    // so make sure we get a compaction when doing a load, but keep around some
    // files in the store
    conf.setInt("hbase.hstore.compaction.min", 10);
    conf.setInt("hbase.hstore.compactionThreshold", 10);
    // block writes if we get to 12 store files
    conf.setInt("hbase.hstore.blockingStoreFiles", 12);
    // drop the number of attempts for the hbase admin
    UTIL.getConfiguration().setInt("hbase.client.retries.number", 1);
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
    assertFalse(UTIL.getHBaseAdmin().getArchivingEnabled(TABLE_NAME));

    // get the RS and region serving our table
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    List<HRegion> servingRegions = UTIL.getHBaseCluster().getRegions(TABLE_NAME);
    // make sure we only have 1 region serving this table
    assertEquals(1, servingRegions.size());
    HRegion region = servingRegions.get(0);
    HRegionServer hrs = UTIL.getRSForFirstRegionInTable(TABLE_NAME);
    FileSystem fs = hrs.getFileSystem();

    // enable archiving
    admin.enableHFileBackup(TABLE_NAME);
    assertTrue(UTIL.getHBaseAdmin().getArchivingEnabled(TABLE_NAME));

    // put some load on the table and make sure that files do get archived
    loadAndCompact(region);

    // check that we actually have some store files that were archived
    HFileArchiveMonitor monitor = hrs.getHFileArchiveMonitor();
    Store store = region.getStore(TEST_FAM);
    Path storeArchiveDir = HFileArchiveTestingUtil.getStoreArchivePath(UTIL.getConfiguration(),
      region, store);

    // check the archive
    assertTrue(fs.exists(storeArchiveDir));
    assertTrue(fs.listStatus(storeArchiveDir).length > 0);

    // now test that we properly stop backing up
    LOG.debug("Stopping backup and testing that we don't archive");
    admin.disableHFileBackup(STRING_TABLE_NAME);
    int counter = 0;
    while (monitor.keepHFiles(STRING_TABLE_NAME)) {
      LOG.debug("Waiting for archive change to propaate");
      Thread.sleep(100);
      // max tries to propagate - if not, something is probably horribly
      // wrong with zk/notification
      assertTrue("Exceeded max tries to propagate hfile backup changes", counter++ < maxTries);
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
    admin.enableHFileBackup(TABLE_NAME);
    admin.disableHFileBackup();
    assertFalse(UTIL.getHBaseAdmin().getArchivingEnabled(TABLE_NAME));
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
    TableHFileArchiveTracker tracker = hrs.hfileArchiveTracker;

    // 2. create the archiving enabled znode
    ZKUtil.createAndFailSilent(zkw, zkw.archiveHFileZNode);

    // 3. now turn on archiving for the test table
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    admin.enableHFileBackup(TABLE_NAME);

    // 4. make sure that archiving is enabled for that tracker
    assertTrue(tracker.keepHFiles(STRING_TABLE_NAME));
  }

  /**
   * Test that we do synchronously start archiving and not return until we are
   * done
   */
  @Test
  public void testSynchronousArchiving() throws Exception {
    HBaseAdmin admin = UTIL.getHBaseAdmin();

    // 1. turn on hfile backups
    LOG.debug("----Starting archiving");
    admin.enableHFileBackup(TABLE_NAME);
    assertTrue(UTIL.getHBaseAdmin().getArchivingEnabled(TABLE_NAME));

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
    assertFalse("Table 'other table' should not be archived - it doesn't exist!", UTIL
        .getHBaseAdmin().getArchivingEnabled(Bytes.toBytes("other table")));

    // 4. now prevent one of the regionservers from archiving, which should
    // cause archiving to fail
    // make sure all archiving is off
    admin.disableHFileBackup();
    assertFalse(admin.getArchivingEnabled(TABLE_NAME));

    // then hack the RS to not do any registration
    HRegionServer hrs = UTIL.getRSForFirstRegionInTable(TABLE_NAME);
    HFileArchiveTableMonitor orig = hrs.hfileArchiveTracker.getMonitor();
    // set the monitor so it doesn't attempt to register the regionserver
    hrs.hfileArchiveTracker.setTableMonitor(new SimpleHFileArchiveTableMonitor(hrs, UTIL
        .getZooKeeperWatcher()));

    // try turning on archiving, but it should fail
    try {
      admin.enableHFileBackup(TABLE_NAME);
      fail("Shouldn't have been able to finish archiving");
    } catch (IOException e) {
      // reset the tracker, if we don't get an exception, then everything breaks
      hrs.hfileArchiveTracker.setTableMonitor(orig);
    }
  }

  /**
   * Test the advanced case where we turn on archiving and the region propagates
   * the change down to the store
   */
  @SuppressWarnings("null")
  // timeout = 200 sec - if it takes longer, something is seriously borked with
  // the minicluster.
  @Test(timeout = 200000)
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
    admin.enableHFileBackup(TABLE_NAME);

    Path storeArchiveDir = HFileArchiveTestingUtil.getStoreArchivePath(UTIL.getConfiguration(),
      region, store);

    // make sure we block the store from compacting/flushing files in the middle
    // of our copy of the store files
    List<StoreFile> origFiles = null;
    FileStatus[] originals = null;
    List<Path> copiedStores = null;
    FileStatus[] archivedFiles = null;
    // wait for all the compactions to finish
    waitOnCompactions(store);

    // lock the store for compactions
    boolean done = false;
    int tries = 0;
    // this is a couple times to ensure that it wasn't just a compaction issue
    while (!done && tries++ < maxTries) {
      // wait on memstore flushes to finish
      region.waitForFlushesAndCompactions();
      synchronized (store.filesCompacting) {
        synchronized (store.flushLock) {
          // if there are files unlock it and try again
          if (store.filesCompacting.size() > 0) {
            LOG.debug("Got some more files, waiting on compaction to finish again.");
            continue;
          }
          LOG.debug("Locked the store");

          // make sure we don't have any extra archive files from random
          // compactions in the middle of the test
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

          // get the original store files before compaction
          LOG.debug("------Original store files:");
          originals = fs.listStatus(store.getHomedir());
          for (FileStatus f : originals) {
            LOG.debug("\t" + f.getPath());
          }
          // copy the original store files so we can use them for testing
          // overwriting store files with the same name below
          origFiles = store.getStorefiles();
          copiedStores = new ArrayList<Path>(origFiles.size());
          Path temproot = new Path(hrs.getRootDir(), "store_copy");
          for (StoreFile f : origFiles) {
            if (!fs.exists(f.getPath())) continue;

            // do the actually copy of the file
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
          compactRegion(region, TEST_FAM);

          // then get the archived store files
          LOG.debug("----------Archived store files after compaction (" + storeArchiveDir + "):");
          archivedFiles = fs.listStatus(storeArchiveDir);
          for (FileStatus f : archivedFiles) {
            LOG.debug("\t" + f.getPath());
          }

          // compare the archive to the original, and then try again if not
          // equal since a compaction may have been finishing
          if (!compareArchiveToOriginal(originals, archivedFiles, fs, false)) {
            LOG.debug("Archive doesn't match, trying again.");
            done = false;
          } else done = true;
        }
      }
    }
    LOG.debug("Unlocked the store.");
    for (FileStatus f : archivedFiles) {
      LOG.debug("\t" + f.getPath());
    }
    assertArchiveEqualToOriginal(originals, archivedFiles, fs);
    assertTrue("Tried too many times, something is messed up in the check logic", done);
    LOG.debug("Archive matches originals.");

    // 3. Now copy back in the store files and trigger another compaction

    // lock again so we don't interfere with a compaction/flush
    waitOnCompactions(store);
    done = false;
    tries = 0;
    while (!done && tries++ < maxTries) {
      // wait for flushes
      region.waitForFlushesAndCompactions();
      synchronized (store.filesCompacting) {
        synchronized (store.flushLock) {
          LOG.debug("Locked the store");
          if (store.filesCompacting.size() > 0) {
            LOG.debug("Got some more files, waiting on compaction to finish again.");
            // hack sleep, but should help if we get a lot of compactions
            Thread.sleep(100);
            continue;
          }

          // delete the store directory (just in case)
          fs.delete(store.getHomedir(), true);

          // and copy back in the original store files (from before)
          fs.mkdirs(store.getHomedir());
          for (int i = 0; i < copiedStores.size(); i++) {
            fs.rename(copiedStores.get(i), origFiles.get(i).getPath());
          }
          // now archive the files again
          LOG.debug("Removing the store files again.");
          RegionDisposer.disposeStoreFiles(hrs, store.getHRegion(), store.conf, store.getFamily()
              .getName(), origFiles);

          // ensure the files match to originals, but with a backup directory
          LOG.debug("Checking originals vs. backed up (from archived) versions");
          archivedFiles = fs.listStatus(storeArchiveDir);

          // check equality to make sure a compaction didn't mess us up
          if (!compareArchiveToOriginal(originals, archivedFiles, fs, true)) {
            // loop again, to check the new files
            LOG.debug("Archive doesn't match, trying again.");
            done = false;
          } else done = true;
          done = true;
        }
      }
    }
    assertArchiveEqualToOriginal(originals, archivedFiles, fs, true);
    assertTrue("Tried too many times, something is messed up in the check logic", done);
  }

  private void waitOnCompactions(Store store) throws Exception {
    // busy loop waiting on files to compact to be empty
    while (store.filesCompacting.size() > 0) {
      Thread.sleep(100);
    }
  }

  @Test
  public void testRegionSplitAndArchive() throws Exception {
    // start archiving
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    admin.enableHFileBackup(TABLE_NAME);

    // get the current store files for the region
    final List<HRegion> servingRegions = UTIL.getHBaseCluster().getRegions(TABLE_NAME);
    // make sure we only have 1 region serving this table
    assertEquals(1, servingRegions.size());
    HRegion region = servingRegions.get(0);

    // and that there is only one store in the region
    assertEquals(1, region.getStores().size());
    Store store = region.getStores().get(TEST_FAM);

    // prep the store files so we get some files
    LOG.debug("Loading store files");
    // prepStoreFiles(admin, store, 3);
    UTIL.loadRegion(region, TEST_FAM);

    // get the files before compaction
    FileSystem fs = region.getRegionServerServices().getFileSystem();

    // delete out the current archive files, just for ease of comparison
    // and synchronize to make sure we don't clobber another compaction
    Path storeArchiveDir = HFileArchiveTestingUtil.getStoreArchivePath(UTIL.getConfiguration(),
      region, store);

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
    servingRegions.clear();
    servingRegions.addAll(UTIL.getHBaseCluster().getRegions(TABLE_NAME));
    // make sure we now have 2 regions serving this table
    assertEquals(2, servingRegions.size());

    // now check to make sure that those regions will also archive
    Collection<HRegionServer> regionservers = Collections2.filter(Collections2.transform(UTIL
        .getMiniHBaseCluster().getRegionServerThreads(),
      new Function<JVMClusterUtil.RegionServerThread, HRegionServer>() {

        @Override
        public HRegionServer apply(RegionServerThread input) {
          return input.getRegionServer();
        }
      }), new Predicate<HRegionServer>() {

      @Override
      public boolean apply(HRegionServer input) {
        // get the names of the regions hosted by the rs
        Collection<String> regions;
        regions = Collections2.transform(input.getOnlineRegions(TABLE_NAME),
          new Function<HRegion, String>() {
            @Override
            public String apply(HRegion input) {
              return input.getRegionInfo().getEncodedName();
            }
          });

        // then check to make sure this RS is serving one of the serving regions
        boolean found = false;
        for (HRegion region : servingRegions) {
          if (regions.contains(region.getRegionInfo().getEncodedName())) {
            found = true;
            break;
          }
        }
        return found;

      }
    });

    assertTrue(regionservers.size() > 0);

    // check each of the region servers to make sure it has got the update
    for (HRegionServer serving : regionservers) {
      assertTrue("RegionServer:" + serving + " hasn't been included in backup",
        serving.hfileArchiveTracker.keepHFiles(STRING_TABLE_NAME));
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
    throw last;
  }

  /**
   * Compact all the store files in a given region.
   */
  private void compactRegion(HRegion region, byte[] family) throws IOException {
    Store store = region.getStores().get(TEST_FAM);
    store.compactRecentForTesting(store.getStorefiles().size());
  }
}
