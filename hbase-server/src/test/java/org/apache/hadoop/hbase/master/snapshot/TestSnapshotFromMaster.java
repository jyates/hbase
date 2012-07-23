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

import static org.apache.hadoop.hbase.master.cleaner.CleanerTestUtils.addHFileCleanerChecking;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.master.cleaner.CleanerTestUtils;
import org.apache.hadoop.hbase.master.cleaner.HFileCleaner;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.snapshot.RegionServerSnapshotHandler;
import org.apache.hadoop.hbase.snapshot.GlobalSnapshotUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test the master-related aspects of a snapshot
 */
@Category(MediumTests.class)
public class TestSnapshotFromMaster {

  private static final Log LOG = LogFactory.getLog(TestSnapshotFromMaster.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final int NUM_RS = 2;
  private static final String STRING_TABLE_NAME = "test";
  private static final byte[] TEST_FAM = Bytes.toBytes("fam");
  private static final byte[] TABLE_NAME = Bytes.toBytes(STRING_TABLE_NAME);
  // refresh the cache every 1/2 second
  private static final long cacheRefreshPeriod = 500;

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
    conf.setInt("hbase.hstore.compaction.min", 5);
    conf.setInt("hbase.hstore.compactionThreshold", 5);
    // block writes if we get to 12 store files
    conf.setInt("hbase.hstore.blockingStoreFiles", 12);
    // drop the number of attempts for the hbase admin
    conf.setInt("hbase.client.retries.number", 1);
    // set the number of threads to use for taking the snapshot
    conf.setInt(RegionServerSnapshotHandler.SNAPSHOT_REQUEST_THREADS, 2);
    // set the only HFile cleaner as the snapshot cleaner
    conf.setStrings(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS,
      SnapshotHFileCleaner.class.getCanonicalName());
    addHFileCleanerChecking(conf);
    conf.setLong(SnapshotHFileCleaner.HFILE_CACHE_REFRESH_PERIOD_CONF_KEY, cacheRefreshPeriod);
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
      // NOOP;
    }
  }

  /**
   * Test that the snapshot hfile archive cleaner works correctly
   */
  @Test
  public void testSnapshotHFileArchiving() throws Exception {
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    // make sure we don't fail on listing snapshots
    assertEquals("Have some previous snapshots", 0, admin.listSnapshots().length);
    // load the table
    UTIL.loadTable(new HTable(UTIL.getConfiguration(), TABLE_NAME), TEST_FAM);

    // take a snapshot of the table
    byte[] snapshotName = Bytes.toBytes("snapshot");
    admin.snapshot(snapshotName, TABLE_NAME);

    // list the snapshot
    SnapshotDescriptor[] snapshots = admin.listSnapshots();
    assertEquals("Should only have 1 snapshot", 1, snapshots.length);
    assertArrayEquals(snapshotName, snapshots[0].getSnapshotName());
    assertArrayEquals(TABLE_NAME, snapshots[0].getTableName());

    // make sure we get a compaction
    List<HRegion> regions = UTIL.getHBaseCluster().getRegions(TABLE_NAME);
    for (HRegion region : regions) {
      region.compactStores();
    }

    // make sure the cleaner has run
    LOG.debug("Running hfile cleaners");
    CleanerTestUtils.ensureHFileCleanersRun(UTIL, cacheRefreshPeriod);

    // check that the files in the archive contain the ones that we need for the snapshot
    Configuration conf = UTIL.getConfiguration();
    Path rootDir = FSUtils.getRootDir(conf);
    FileSystem fs = FSUtils.getCurrentFileSystem(conf);
    // get the snapshot files for the table
    Path snapshotTable = SnapshotDescriptor.getCompletedSnapshotDir(snapshotName, rootDir);
    FileStatus[] snapshotHFiles = GlobalSnapshotUtils.listHFiles(fs, snapshotTable);
    LOG.debug("Have snapshot hfiles:");
    for (FileStatus file : snapshotHFiles) {
      LOG.debug(file.getPath());
    }
    // get the archived files for the table
    Collection<String> files = getArchivedHFiles(conf, rootDir, fs, STRING_TABLE_NAME);

    // and make sure that there is a proper subset
    for (FileStatus file : snapshotHFiles) {
      assertTrue("Archived hfiles " + files + " is missing snapshot file:" + file.getPath(),
        files.contains(Reference.getDeferencedHFileName(file.getPath().getName())));
    }

    // delete the existing snapshot
    admin.deleteSnapshot(snapshotName);
    snapshots = admin.listSnapshots();
    assertEquals("Should only have deleted all snapshots", 0, snapshots.length);

    // make sure that we don't keep around the hfiles that aren't in a snapshot
    // make sure we wait long enough to refresh the snapshot hfile
    Thread.sleep(cacheRefreshPeriod + 10);
    // run the cleaner again
    LOG.debug("Running hfile cleaners");
    CleanerTestUtils.ensureHFileCleanersRun(UTIL, cacheRefreshPeriod);

    files = getArchivedHFiles(conf, rootDir, fs, STRING_TABLE_NAME);
    assertEquals("Still have some hfiles in the archive, when their snapshot has been deleted.", 0,
      files.size());
  }

  /**
   * @return all the HFiles for a given table that have been archived
   * @throws IOException on expected failure
   */
  private final Collection<String> getArchivedHFiles(Configuration conf, Path rootDir,
      FileSystem fs, String tableName) throws IOException {
    Path tableArchive = HFileArchiveUtil.getTableArchivePath(conf, new Path(rootDir, tableName));
    FileStatus[] archivedHFiles = GlobalSnapshotUtils.listHFiles(fs, tableArchive);
    List<String> files = new ArrayList<String>(archivedHFiles.length);
    LOG.debug("Have archived hfiles:");
    for (FileStatus file : archivedHFiles) {
      LOG.debug(file.getPath());
      files.add(Reference.getDeferencedHFileName(file.getPath().getName()));
    }
    // sort the archived files

    Collections.sort(files);
    return files;
  }
}