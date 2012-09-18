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

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.IsSnapshotDoneRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.IsSnapshotDoneResponse;
import org.apache.hadoop.hbase.regionserver.snapshot.RegionServerSnapshotHandler;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.snapshot.exception.UnknownSnapshotException;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

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

  /**
   * Test that the contract from the master for checking on a snapshot are valid.
   */
  @Test(timeout = 15000)
  @Category(MediumTests.class)
  public void testIsDoneContract() throws Exception {
    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    master.getSnapshotManager().reset();
    IsSnapshotDoneRequest.Builder builder = IsSnapshotDoneRequest.newBuilder();
    String snapshotName = "asyncExpectedFailureTest";

    // check that we get an exception when looking up snapshot where one hasn't happened
    SnapshotTestingUtils.expectSnapshotDoneException(master, builder.build(),
      UnknownSnapshotException.class);

    // and that we get the same issue, even if we specify a name
    SnapshotDescription desc = SnapshotDescription.newBuilder().setName(snapshotName).build();
    builder.setSnapshot(desc);
    SnapshotTestingUtils.expectSnapshotDoneException(master, builder.build(),
      UnknownSnapshotException.class);

    // set a mock handler to simulate a snapshot
    TableSnapshotHandler mockHandler = Mockito.mock(TableSnapshotHandler.class);
    Mockito.doNothing().when(mockHandler).failOnError();
    Mockito.when(mockHandler.getSnapshot()).thenReturn(desc);
    Mockito.when(mockHandler.getFinished()).thenReturn(new Boolean(true));

    master.getSnapshotManager().setSnapshotHandler(mockHandler);

    // if we do a lookup without a snapshot name, we should fail - you should always know your name
    builder = IsSnapshotDoneRequest.newBuilder();
    SnapshotTestingUtils.expectSnapshotDoneException(master, builder.build(),
      UnknownSnapshotException.class);

    // then do the lookup for the snapshot that it is done
    builder.setSnapshot(desc);
    IsSnapshotDoneResponse response = master.isSnapshotDone(null, builder.build());
    assertTrue("Snapshot didn't complete when it should have.", response.getDone());

    // now try the case where we are looking for a snapshot we didn't take
    builder.setSnapshot(SnapshotDescription.newBuilder().setName("Not A Snapshot").build());
    SnapshotTestingUtils.expectSnapshotDoneException(master, builder.build(),
      UnknownSnapshotException.class);
  }
}