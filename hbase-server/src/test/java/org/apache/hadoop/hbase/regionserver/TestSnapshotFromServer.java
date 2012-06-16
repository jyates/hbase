/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.TestSnapshotFromClient;
import org.apache.hadoop.hbase.regionserver.snapshot.RegionServerSnapshotHandler;
import org.apache.hadoop.hbase.regionserver.snapshot.monitor.SnapshotFaultInjector;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test different snapshot aspects from the server-side
 */
@Category(MediumTests.class)
public class TestSnapshotFromServer {

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
    conf.setInt(HConstants.SNAPSHOT_REQUEST_THREADS, 2);
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
  public void testFaultInRegion() throws Exception {
    LOG.debug("------- Starting Snapshot Fault test -------------");
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

    HRegionServer server = UTIL.getRSForFirstRegionInTable(TABLE_NAME);
    RegionServerSnapshotHandler handler = server.snapshotHandler;
    final boolean[] faulted = new boolean[] { false };

    handler.addFaultInjector(new SnapshotFaultInjector() {
      @Override
      public boolean injectFault(Class<?> clazz) {
        if (clazz.equals(HRegion.class)) {
          LOG.debug("injecting fault for:" + clazz);
          faulted[0] = true;
          return true;
        }
        LOG.debug("NOT injecting fault for:" + clazz);
        return false;
      }
    });

    // test creating the snapshot
    byte[] snapshotName = Bytes.toBytes("snapshot");
    try {
      admin.snapshot(snapshotName, TABLE_NAME);
      fail("Snapshot should have been failed by the fault injection");
    } catch (IOException e) {
      // ignore - this should happen
    }
    assertTrue("Snapshot wasn't faulted by the injection handler", faulted[0]);
    SnapshotDescriptor snapshot = new SnapshotDescriptor(snapshotName, TABLE_NAME);
    // check the directory structure
    FileSystem fs = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getFileSystem();
    Path rootDir = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();
    // check that the snapshot dir was created
    logFSTree(new Path(UTIL.getConfiguration().get(HConstants.HBASE_DIR)));
    Path snapshotDir = SnapshotDescriptor.getSnapshotDir(rootDir);
    assertTrue(fs.exists(snapshotDir));
    // check that we cleanup after ourselves on failure
    assertEquals("There is not just one directory in the snapshot dir", 1,
      fs.listStatus(snapshotDir).length);
    Path snapshotTmp = SnapshotDescriptor.getWorkingSnapshotDir(snapshot, rootDir);
    assertFalse("Working snapshot directory still exists!", fs.exists(snapshotTmp));
    assertEquals("There are still temporary snapshot files", 0,
      fs.listStatus(fs.listStatus(snapshotDir)[0].getPath()).length);

    // make sure we don't have any snapshots
    assertEquals(0, admin.listSnapshots().length);
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
