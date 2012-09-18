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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
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
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TestSnapshotFromClient;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.snapshot.SnapshotCleaner;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.IsSnapshotDoneRequest;
import org.apache.hadoop.hbase.regionserver.snapshot.RegionServerSnapshotHandler;
import org.apache.hadoop.hbase.server.errorhandling.impl.CheckableFaultInjector;
import org.apache.hadoop.hbase.server.errorhandling.impl.ExceptionOrchestratorFactory;
import org.apache.hadoop.hbase.server.errorhandling.impl.FaultInjectionPolicy;
import org.apache.hadoop.hbase.server.errorhandling.impl.PoliciedFaultInjector;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.snapshot.error.ContainsClassInjectionPolicy;
import org.apache.hadoop.hbase.snapshot.error.SnapshotFailureInjector;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.exception.SnapshotCreationException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import com.google.protobuf.ServiceException;

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
  /** check once a second to see if the snapshot is done, when running async, in ms */
  private static final long ASYNC_WAIT_PERIOD = 1000;
  private static final FaultInjectionPolicy CONTAINS_HREGION = new ContainsClassInjectionPolicy(
      HRegion.class);

  /**
   * Setup the config for the cluster
   * @throws Exception on failure
   */
  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(NUM_RS);

    // setup policy checking for the stores being swapped

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
    conf.setInt("hbase.client.retries.number", 10);
    // set the number of threads to use for taking the snapshot
    conf.setInt(RegionServerSnapshotHandler.SNAPSHOT_REQUEST_THREADS, 2);
  }

  @Before
  public void setup() throws Exception {
    UTIL.createTable(TABLE_NAME, TEST_FAM);
  }

  @After
  public void tearDown() throws Exception {
    // remove any added fault injectors (just in case)
    ExceptionOrchestratorFactory.clearFaults();

    UTIL.deleteTable(TABLE_NAME);
    // and cleanup the archive directory
    try {
      UTIL.getTestFileSystem().delete(new Path(UTIL.getDefaultRootDirPath(), ".archive"), true);
      SnapshotCleaner.ensureCleanerRuns();
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

  private void runSnapshotWithFault(Callable<Void> snapshotRunner, String snapshotName,
      final FaultInjectionPolicy policy) throws Exception {
    LOG.debug("------- Starting Snapshot Fault test -------------");
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    // make sure we don't fail on listing snapshots
    SnapshotTestingUtils.assertNoSnapshots(admin);
    // load the table so we have some data
    UTIL.loadTable(new HTable(UTIL.getConfiguration(), TABLE_NAME), TEST_FAM);
    waitForTableToStabilize(TABLE_NAME);

    // create an injector we can check later and that throws a basic snapshot failure
    CheckableFaultInjector<HBaseSnapshotException> injector = new SnapshotFailureInjector(policy,
        UTIL);
    // make sure we reset the fault state for each pass
    CheckableFaultInjector.reset();
    ExceptionOrchestratorFactory.addFaultInjector(injector);

    // test creating the snapshot
    try {
      snapshotRunner.call();
      fail("Snapshot should have been failed by the fault injection");
    } catch (Exception e) {
      LOG.debug("Got expected exception", e);
    } finally {
      // remove any added fault injectors
      ExceptionOrchestratorFactory.clearFaults();
    }
    assertTrue("Snapshot wasn't faulted by the injection handler",
      CheckableFaultInjector.getFaulted());

    SnapshotDescription snapshot = SnapshotDescription.newBuilder()
.setName(snapshotName)
        .setTable(Bytes.toString(TABLE_NAME)).build();
    checkSnapshotDirectoryStructure(admin, snapshot);

    // check that we get an error back from the master when checking status
    LOG.debug("Checking for failed snapshot returns exception on async lookup.");
    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    IsSnapshotDoneRequest request = IsSnapshotDoneRequest.newBuilder().setSnapshot(snapshot)
        .build();
    try {
      master.isSnapshotDone(null, request);
      fail("Master should throw exception when checking for a failed snapshot.");
    } catch (ServiceException e) {
      assertTrue("Didn't get a snapshot creation exception when looking up snapshot state.",
        (ProtobufUtil.getRemoteException(e)) instanceof SnapshotCreationException);
      LOG.debug("Correctly got exception for failed snapshot", e);
    }
  }

  /**
   * Check that the snapshot was correctly created and that the working directory was correctly
   * removed. Ensures that the {@link SnapshotCleaner} runs before doing any checking.
   * @para admin admin for hbase cluster to check for existing snapshots
   * @param snapshot snapshot to check
   */
  private void checkSnapshotDirectoryStructure(HBaseAdmin admin, SnapshotDescription snapshot)
      throws IOException {
    // make sure the working directory cleaner runs
    SnapshotCleaner.ensureCleanerRuns();

    // check the directory structure
    FileSystem fs = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getFileSystem();
    Path rootDir = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();
    // check that the snapshot dir was created
    logFSTree(new Path(UTIL.getConfiguration().get(HConstants.HBASE_DIR)));
    Path snapshotDir = SnapshotDescriptionUtils.getSnapshotDir(rootDir);
    assertTrue(fs.exists(snapshotDir));
    // check that we cleanup after ourselves on failure
    assertEquals("There is not just one directory in the snapshot dir", 1,
      fs.listStatus(snapshotDir).length);
    Path workingSnapshotDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(snapshot, rootDir);
    assertFalse("Working snapshot directory (" + workingSnapshotDir + ") still exists!",
      fs.exists(workingSnapshotDir));

    // make sure we don't have any snapshots
    SnapshotTestingUtils.assertNoSnapshots(admin);
  }


  /**
   * Run a snapshot with a concurrent operation
   * @param snapshotRunner runner to launch the snapshot (allows global or timestamp snapshot)
   * @param snapshotName name of the snapshot being taken
   * @param op concurrent operation to run while taking the snapshot
   * @param requireLogs if the output requires the hlogs to be present (for validation)
   * @param loadTable if the table should be loaded before the test
   * @param andPolicies fault policies to AND with the concurrent operation policy (which is
   *          necessary to block the running snapshot). If all these policies pass, then we block
   *          the snapshot (but don't inject a fault).
   * @throws Exception
   */
  private void runSnapshotWithConcurrentOperation(final Callable<Void> snapshotRunner,
      String snapshotName, ConcurrentSnapshotOperation op, boolean requireLogs,
      boolean loadTable, FaultInjectionPolicy andPolicy) throws Exception {
    final HBaseAdmin admin = UTIL.getHBaseAdmin();
    // make sure we don't fail on listing snapshots
    SnapshotTestingUtils.assertNoSnapshots(admin);
    // load the table so we have some data
    HTable table = new HTable(UTIL.getConfiguration(), STRING_TABLE_NAME);

    // only load the table if the test requires
    if (loadTable) {
      LOG.debug("--- Snapshot test loading table.");
      UTIL.loadTable(table, TEST_FAM);
      waitForTableToStabilize(TABLE_NAME);
    }

    // apply the test operation to the table
    LOG.debug("Running concurrent operation prepare...");
    op.setHTable(table);
    op.prepare();

    final CountDownLatch continueSnapshot = new CountDownLatch(1);
    ConcurrentOperationPolicy policy = new ConcurrentOperationPolicy();
    // setup the injector continue latch
    policy.setSetContinueSnapshotLatch(continueSnapshot);
    // get the snapshot blocked latch
    final CountDownLatch snapshotBlocked = policy.getSnapshotBlockedLatch();

    // tie the concurrent policy to the conditional policy
    andPolicy.and(policy);

    // create a simple policy to handle fault injection
    final Pair<Boolean, String> injectorWorked = new Pair<Boolean, String>(true, null);
    PoliciedFaultInjector<Exception> injector = new PoliciedFaultInjector<Exception>(policy) {
      @Override
      protected Pair<Exception, Object[]> getInjectedError(StackTraceElement[] trace) {
        injectorWorked.setFirst(false);
        String msg = "Shouldn't haver faulted during a concurrent task!";
        injectorWorked.setSecond(msg);
        // make sure fail message makes it into the logs
        fail(msg);
        return new Pair<Exception, Object[]>(new RuntimeException("unexpected fault!"), null);
      }
    };
    ExceptionOrchestratorFactory.addFaultInjector(injector);

    // take the snapshot async
    final Pair<Boolean, String> startWorked = new Pair<Boolean, String>(true, null);
    Thread thread = new Thread() {
      @Override
      public void run() {
        try {
          snapshotRunner.call();
        } catch (Exception e) {
          startWorked.setFirst(false);
          String msg = "Snapshot couldn't be completed async: " + e.getMessage();
          startWorked.setSecond(msg);
          // make sure the fail message makes it into the logs
          fail(msg);
        } finally {
          // make sure we preceed on the main thread, in the worst case
          snapshotBlocked.countDown();
        }
        // finally indicate that we have completed the snapshot
      }
    };
    thread.start();

    // wait for the preceed latch so we are sure the snapshot has started
    LOG.debug("Waiting for snapshot to reach blocking point.");
    snapshotBlocked.await();
    LOG.debug("Able to proceed with verify...");
    op.verify();

    // release the snapshot lock
    LOG.debug("Completed verify, counting down snapshot latch so snapshot can complete.");
    continueSnapshot.countDown();

    // wait for the snapshot-taking thread to finish
    LOG.debug("Waiting for snapshot to complete.");
    thread.join();
    LOG.debug("Snapshot completed!");
    // make sure we didn't get a fault while running the snapshot
    assertTrue(
      "Snapshot should not be faulted during a blocking call:" + injectorWorked.getSecond(),
      injectorWorked.getFirst());
    // make sure we started the snapshot correctly
    assertTrue(startWorked.getSecond(), startWorked.getFirst());
    logFSTree(new Path(UTIL.getConfiguration().get(HConstants.HBASE_DIR)));

    // test that we can delete the snapshot
    SnapshotTestingUtils.cleanupSnapshot(admin, snapshotName);
  }

  // TODO test failing snapshots via abort

  // TODO test failing snapshots via zk error

  /**
   * Wait for any compactions/flushes to complete on the table
   * @param tableName table to wait on
   */
  private void waitForTableToStabilize(byte[] tableName) throws Exception {
    // and wait until everything stabilizes
    HRegionServer rs = UTIL.getRSForFirstRegionInTable(tableName);
    List<HRegion> onlineRegions = rs.getOnlineRegions(tableName);
    for (HRegion region : onlineRegions) {
      region.waitForFlushesAndCompactions();
    }
  }

  private abstract static class ConcurrentSnapshotOperation {
    protected HTable table;

    /**
     * Set the {@link HTable} on which we are snapshotting. Must be called before {@link #prepare()}
     * @param snapshotting
     */
    public void setHTable(HTable snapshotting) {
      this.table = snapshotting;
    }

    public abstract void prepare() throws Exception;

    public abstract void verify() throws Exception;
  }

  /**
   * Read from the test table while
   * {@link #runSnapshotWithConcurrentOperation(Callable, byte[], ConcurrentSnapshotOperation, boolean, boolean, FaultInjectionPolicy...)}
   */
  private static final ConcurrentSnapshotOperation CONCURRENT_READ_OPERATION = new ConcurrentSnapshotOperation() {
    Result r;
    private final Get get = new Get(new byte[] { 'a', 'b', 'c' });

    @Override
    public void prepare() throws Exception {
      r = table.get(get);
    }

    @Override
    public void verify() throws Exception {
      Result other = table.get(get);

      if (r.list().equals(other.list())) {
        LOG.debug("Finish equals");
      } else {
        LOG.debug("NOT equals from output!");
      }
      assertEquals("Obatined result not equal to stored result", r.list(), other.list());
    }
  };

  ConcurrentSnapshotOperation CONCURRENT_WRITE_OPERATION = new ConcurrentSnapshotOperation() {
    @Override
    public void prepare() throws Exception {
    }

    @Override
    public void verify() throws Exception {
      LOG.debug("Verifying snapshot writing.");
      byte[] row = new byte[] { 'a', 'b', 'c' };
      long timestamp = Long.MAX_VALUE - 1000;
      KeyValue expected = new KeyValue(row, TEST_FAM, new byte[0], timestamp,
          Bytes.toBytes("TEST_VALUE"));
      Put p = new Put(row);
      p.add(expected);
      // HTable table = new HTable(UTIL.getConfiguration(), STRING_TABLE_NAME);
      LOG.debug("Putting: " + p + " in table: " + table);
      table.put(p);
      table.flushCommits();
      LOG.debug("Put: " + p + " in table: " + table);

      Get g = new Get(row);
      // g.setTimeStamp(timestamp);
      Result r = table.get(g);
      assertEquals("Obatined result not equal to stored result", p.getFamilyMap().get(TEST_FAM),
        r.list());
    }
  };

  /**
   * Flush a region of the table while
   * {@link #runSnapshotWithConcurrentOperation(Callable, byte[], ConcurrentSnapshotOperation, boolean, boolean, FaultInjectionPolicy...)}
   */
  private static final ConcurrentSnapshotOperation CONCURRENT_FLUSH_OPERATION = new ConcurrentSnapshotOperation() {
    @Override
    public void prepare() throws Exception {
    }

    @Override
    public void verify() throws Exception {
      List<HRegion> regions = UTIL.getMiniHBaseCluster().getRegions(table.getTableName());
      LOG.debug("--- Jesse checking flushing");
      assertTrue("Region didn't flush while snapshotting", regions.get(0).flushcache());
    }
  };

  private static class ConcurrentOperationPolicy extends FaultInjectionPolicy {
    private static final Log LOG = LogFactory.getLog(ConcurrentOperationPolicy.class);
    CountDownLatch continueSnapshot;
    private final CountDownLatch snapshotBlocked = new CountDownLatch(1);

    public void setSetContinueSnapshotLatch(CountDownLatch continueSnapshot) {
      this.continueSnapshot = continueSnapshot;
    }

    public CountDownLatch getSnapshotBlockedLatch() {
      return this.snapshotBlocked;
    }

    @Override
    protected boolean checkForFault(StackTraceElement[] stack) {
      try {
        snapshotBlocked.countDown();
        continueSnapshot.await();
      } catch (InterruptedException e) {
        LOG.error("Interrupted while waiting for latch!");
        throw new RuntimeException(e);
      }
      return false;
    }
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