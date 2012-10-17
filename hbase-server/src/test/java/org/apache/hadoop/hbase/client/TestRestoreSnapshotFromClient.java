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
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test clone/restore snapshots from the client
 */
@Category(LargeTests.class)
public class TestRestoreSnapshotFromClient {
  private static final Log LOG = LogFactory.getLog(TestSnapshotFromClient.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final int NUM_RS = 2;
  private static final String STRING_TABLE_NAME = "test";
  private static final byte[] TEST_FAM = Bytes.toBytes("fam");
  private static final byte[] TABLE_NAME = Bytes.toBytes(STRING_TABLE_NAME);
  private static final byte[] SNAPSHOT_NAME = Bytes.toBytes("snapshot");

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
    conf.setInt("hbase.client.retries.number", 5);
  }

  @Before
  public void setup() throws Exception {
    UTIL.createTable(TABLE_NAME, TEST_FAM);

    HBaseAdmin admin = UTIL.getHBaseAdmin();

    // put some stuff in the table
    UTIL.loadTable(new HTable(UTIL.getConfiguration(), TABLE_NAME), TEST_FAM);

    // take a snapshot
    createSnapshot();

    // put more stuff in the table
    loadMoreData(new HTable(UTIL.getConfiguration(), TABLE_NAME), TEST_FAM);
  }

  @After
  public void tearDown() throws Exception {
    removeSnapshot();

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

  @Test
  public void testRestoreSnapshot() throws Exception {
    HBaseAdmin admin = UTIL.getHBaseAdmin();

    int nrows = UTIL.countRows(new HTable(UTIL.getConfiguration(), TABLE_NAME));
    assertEquals("Number of rows pre-restore", 35152, nrows);

    LOG.debug("FS state before restore:");
      FSUtils.logFileSystemState(UTIL.getTestFileSystem(),
        FSUtils.getRootDir(UTIL.getConfiguration()), LOG);

    admin.disableTable(TABLE_NAME);
    admin.restoreSnapshot(SNAPSHOT_NAME);

    LOG.debug("FS state after restore:");
      FSUtils.logFileSystemState(UTIL.getTestFileSystem(),
        FSUtils.getRootDir(UTIL.getConfiguration()), LOG);


    nrows = UTIL.countRows(new HTable(UTIL.getConfiguration(), TABLE_NAME));
    assertEquals("Number of rows post-restore", 17576, nrows);
  }

  @Test
  public void testCloneSnapshot() throws Exception {
    byte[] tableName = Bytes.toBytes("test-clone");
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    try {
      admin.cloneSnapshot(SNAPSHOT_NAME, tableName);

      int nrows = UTIL.countRows(new HTable(UTIL.getConfiguration(), tableName));
      assertEquals("Number of cloned rows", 17576, nrows);
    } finally {
      UTIL.deleteTable(tableName);
    }
  }

  public void createSnapshot() throws Exception {
    HBaseAdmin admin = UTIL.getHBaseAdmin();

    int nrows = UTIL.countRows(new HTable(UTIL.getConfiguration(), TABLE_NAME));
    assertEquals("Number of rows pre-snapshot", 17576, nrows);

    // make sure we don't fail on listing snapshots
    assertEquals("Have some previous snapshots", 0, admin.listSnapshots().size());

    // disable the table
    admin.disableTable(TABLE_NAME);
    try {
      LOG.debug("FS state before snapshot:");
      FSUtils.logFileSystemState(UTIL.getTestFileSystem(),
        FSUtils.getRootDir(UTIL.getConfiguration()), LOG);

      // take a snapshot of the disabled table
      admin.snapshot(SNAPSHOT_NAME, TABLE_NAME);
      LOG.debug("Snapshot completed.");

      // make sure we have the snapshot
      List<SnapshotDescription> snapshots = admin.listSnapshots();
      assertEquals("Snapshot wasn't taken.", 1, snapshots.size());

      // make sure its a valid snapshot
      FileSystem fs = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getFileSystem();
      Path rootDir = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();
    } finally {
      admin.enableTable(TABLE_NAME);
    }
  }

  private void removeSnapshot() throws Exception {
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    admin.deleteSnapshot(SNAPSHOT_NAME);
    List<SnapshotDescription> snapshots = admin.listSnapshots();
    assertEquals("Snapshot wasn't deleted.", 0, snapshots.size());
  }

  public int loadMoreData(final HTable t, final byte[] f) throws IOException {
    t.setAutoFlush(false);
    byte[] k = new byte[4];
    int rowCount = 0;
    for (byte b1 = 'a'; b1 <= 'z'; b1++) {
      for (byte b2 = 'a'; b2 <= 'z'; b2++) {
        for (byte b3 = 'a'; b3 <= 'z'; b3++) {
          k[0] = 'a';
          k[1] = b1;
          k[2] = b2;
          k[3] = b3;
          Put put = new Put(k);
          put.add(f, null, k);
          t.put(put);
          rowCount++;
        }
      }
    }
    t.flushCommits();
    return rowCount;
  }
}
