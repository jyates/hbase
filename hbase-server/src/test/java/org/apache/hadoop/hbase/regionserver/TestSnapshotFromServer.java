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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TestSnapshotFromClient;
import org.apache.hadoop.hbase.regionserver.snapshot.RegionServerSnapshotHandler;
import org.apache.hadoop.hbase.server.error.ErrorHandlingUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.snapshot.monitor.SnapshotErrorCheckable;
import org.apache.hadoop.hbase.snapshot.monitor.SnapshotFaultInjector;
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
    conf.setInt(RegionServerSnapshotHandler.SNAPSHOT_REQUEST_THREADS, 2);
  }

  @Before
  public void setup() throws Exception {
    UTIL.createTable(TABLE_NAME, TEST_FAM);
  }

  @After
  public void tearDown() throws Exception {
    // remove any added fault injectors
    SnapshotErrorCheckable.clearFaultInjectors();

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

  @Test(timeout = 1000)
  public void testFaultInRegionGlobalSnapshot() throws Exception {
    final HBaseAdmin admin = UTIL.getHBaseAdmin();
    final byte[] snapshotName = Bytes.toBytes("globalSnapshotWithFault");
    Callable<Void> runSnapshot = new Callable<Void>() {

      @Override
      public Void call() throws Exception {
        admin.globalSnapshot(snapshotName, TABLE_NAME);
        return null;
      }
    };

    runSnapshotWithFault(runSnapshot, snapshotName);
  }

  @Test(timeout = 1000)
  public void testFaultInRegionTimestampSnapshot() throws Exception {
    final HBaseAdmin admin = UTIL.getHBaseAdmin();
    final byte[] snapshotName = Bytes.toBytes("timestampSnapshotWithFault");
    Callable<Void> runSnapshot = new Callable<Void>() {

      @Override
      public Void call() throws Exception {
        admin.snapshot(snapshotName, TABLE_NAME);
        return null;
      }
    };

    runSnapshotWithFault(runSnapshot, snapshotName);
  }

  private void runSnapshotWithFault(Callable<Void> snapshotRunner, final byte[] snapshotName)
      throws IOException {
    LOG.debug("------- Starting Snapshot Fault test -------------");
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    // make sure we don't fail on listing snapshots
    assertEquals("Expected no previous snapshots.", 0, admin.listSnapshots().length);
    // load the table so we have some data
    UTIL.loadTable(new HTable(UTIL.getConfiguration(), TABLE_NAME), TEST_FAM);
    // and wait until everything stabilizes
    HRegionServer rs = UTIL.getRSForFirstRegionInTable(TABLE_NAME);
    List<HRegion> onlineRegions = rs.getOnlineRegions(TABLE_NAME);
    for (HRegion region : onlineRegions) {
      region.waitForFlushesAndCompactions();
    }
    final boolean[] faulted = new boolean[] { false };

    SnapshotErrorCheckable.addFaultInjector(new SnapshotFaultInjector() {
      @Override
      public boolean injectFault(StackTraceElement[] stack) {
        if (ErrorHandlingUtils.stackContainsClass(stack, HRegion.class)) {
          LOG.debug("injecting fault for:" + HRegion.class);
          faulted[0] = true;
          return true;
        }
        LOG.debug("NOT injecting fault");
        return false;
      }
    });

    // test creating the snapshot

    try {
      snapshotRunner.call();
      fail("Snapshot should have been failed by the fault injection");
    } catch (Exception e) {
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
    Path workingSnapshotDir = SnapshotDescriptor.getWorkingSnapshotDir(snapshot, rootDir);
    assertFalse("Working snapshot directory still exists!", fs.exists(workingSnapshotDir));

    // make sure we don't have any snapshots
    assertEquals("Snapshot completed incorrectly, found a snapshot", 0,
      admin.listSnapshots().length);
    assertNull("Snapshot master didn't clear sentinel after failed snapshot", UTIL
        .getMiniHBaseCluster().getMaster().getSnapshotManager().getCurrentSnapshotTracker());
  }

  /**
   * Convert a bunch of file status to a string
   * @param listStatus
   * @return
   */
  private String statusToString(FileStatus[] listStatus) {
    if (listStatus == null || listStatus.length == 0) return null;
    List<Path> files = new ArrayList<Path>(listStatus.length);
    for (FileStatus file : listStatus) {
      files.add(file.getPath());
    }
    return files.toString();
  }

  /**
   * Test that we can read from a table mid-global snapshot (which blocks writes)
   * @throws Exception on unexpected failure
   */
  @Test(timeout = 1000)
  public void testNonBlockingReadingInGlobalSnapshot() throws Exception {
    final HBaseAdmin admin = UTIL.getHBaseAdmin();
    final byte[] snapshotName = Bytes.toBytes("globalSnapshotWithReads");
    Callable<Void> runSnapshot = new Callable<Void>() {

      @Override
      public Void call() throws Exception {
        admin.globalSnapshot(snapshotName, TABLE_NAME);
        return null;
      }
    };

    runSnapshotWithReads(runSnapshot, snapshotName, true);
  }

  /**
   * Test that we can read from a table mid-global snapshot (which blocks writes)
   * @throws Exception on unexpected failure
   */
  @Test(timeout = 1000)
  public void testNonBlockingReadingInTimestampSnapshot() throws Exception {
    final HBaseAdmin admin = UTIL.getHBaseAdmin();
    final byte[] snapshotName = Bytes.toBytes("globalSnapshotWithReads");
    Callable<Void> runSnapshot = new Callable<Void>() {

      @Override
      public Void call() throws Exception {
        admin.snapshot(snapshotName, TABLE_NAME);
        return null;
      }
    };

    runSnapshotWithReads(runSnapshot, snapshotName, false);
  }

  private void runSnapshotWithReads(final Callable<Void> snapshotRunner, final byte[] snapshotName,
      boolean requireLogs) throws Exception {
    final HBaseAdmin admin = UTIL.getHBaseAdmin();
    // make sure we don't fail on listing snapshots
    assertEquals("Have snapshots left over from previous test - a previous test didn't cleanup", 0,
      admin.listSnapshots().length);
    // load the table so we have some data
    HTable table = new HTable(UTIL.getConfiguration(), TABLE_NAME);
    UTIL.loadTable(table, TEST_FAM);
    // and wait until everything stabilizes
    HRegionServer rs = UTIL.getRSForFirstRegionInTable(TABLE_NAME);
    List<HRegion> onlineRegions = rs.getOnlineRegions(TABLE_NAME);
    for (HRegion region : onlineRegions) {
      region.waitForFlushesAndCompactions();
    }

    // get some row from the table
    Get get = new Get(new byte[] { 'a', 'b', 'c' });
    Result r = table.get(get);

    // add a fault injector that just blocks until we are done
    final CountDownLatch latch = new CountDownLatch(1);
    final CountDownLatch proceed = new CountDownLatch(1);
    final CountDownLatch completed = new CountDownLatch(1);
    SnapshotErrorCheckable.addFaultInjector(new SnapshotFaultInjector() {
      @Override
      public boolean injectFault(StackTraceElement[] stack) {
        if (ErrorHandlingUtils.stackContainsClass(stack, HRegion.class)) {
          LOG.debug("Fault injector - waiting on latch:" + HRegion.class);
          try {
            proceed.countDown();
            latch.await();
          } catch (InterruptedException e) {
            LOG.error("Interrupted while waiting for latch!");
            return true;
          }
        }
        LOG.debug("NOT injecting fault");
        return false;
      }
    });

    // take the snapshot async
    Thread thread = new Thread() {
      @Override
      public void run() {
        try {
          snapshotRunner.call();
        } catch (Exception e) {
          LOG.error("Snapshot couldn't be completed async");
        } finally {
          // make sure we preceed on the main thread, in the worst case
          proceed.countDown();
        }
        // finally indicate that we have completed the snapshot
        completed.countDown();
      }
    };
    thread.start();

    // wait for the preceed latch so we are sure the snapshot has started
    LOG.debug("Waiting for snapshot to reach blocking point.");
    proceed.await();
    LOG.debug("Able to proceed with read request");
    Result other = table.get(get);

    // release the snapshot lock
    LOG.debug("Counting down snapshot latch so it can complete.");
    latch.countDown();

    if (r.list().equals(other.list())) {
      LOG.debug("Finish equals");
    } else {
      LOG.debug("NOT equals from output!");
    }
    assertEquals("Obatined result not equal to stored result", r.list(), other.list());

    // wait for the snapshot to complete
    LOG.debug("Waiting for snapshot to complete.");
    completed.await();

    LOG.debug("Snapshot completed!");
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
      fs, requireLogs);

    // test that we can delete the snapshot
    admin.deleteSnapshot(snapshotName);

    // make sure we don't have any snapshots
    assertEquals("Snapshot wasn't deleted", 0, admin.listSnapshots().length);
  }

  // TODO test for timestamp snapshot writes

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