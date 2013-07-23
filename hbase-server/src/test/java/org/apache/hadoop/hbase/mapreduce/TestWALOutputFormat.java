/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.PerformanceEvaluation;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the writing to a WAL during a Map/Reduce job works when using the {@link BulkLoadUtils}
 * reducers.
 */
public class TestWALOutputFormat {

  private static Log LOG = LogFactory.getLog(TestWALOutputFormat.class);
  private final static int ROWSPERSPLIT = 1024;

  private static final byte[][] FAMILIES = {
      Bytes.add(PerformanceEvaluation.FAMILY_NAME, Bytes.toBytes("-A")),
      Bytes.add(PerformanceEvaluation.FAMILY_NAME, Bytes.toBytes("-B")) };
  private static final byte[] TABLE_NAME = Bytes.toBytes("TestTable");

  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setupCluster() throws Exception {
    UTIL.startMiniCluster();
    UTIL.startMiniMapReduceCluster();
  }

  @AfterClass
  public static void teardownCluster() throws Exception {
    UTIL.shutdownMiniCluster();
    UTIL.shutdownMiniMapReduceCluster();
  }

  @Test
  public void testKeyValueReducer() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setInt("hbase.hstore.compaction.min", 2);
    TestHFileOutputFormat.generateRandomStartKeys(5);

    Path testDir = UTIL.getDataTestDirOnTestFS("testKeyValueReducer");
    final FileSystem fs = UTIL.getDFSCluster().getFileSystem();
    HBaseAdmin admin = new HBaseAdmin(conf);
    HTable table = UTIL.createTable(TABLE_NAME, FAMILIES);
    assertEquals("Should start with empty table", 0, UTIL.countRows(table));

    // Generate a bulk load file with more rows
    conf.setBoolean("hbase.mapreduce.hfileoutputformat.compaction.exclude", true);
    // enable HLog Output as well
    conf.setBoolean(WALOutputFormat.ENABLE_WAL_CONF_KEY, true);
    runIncrementalPELoad(conf, table, testDir);

    // log the FS state for debugging
    FSUtils.logFileSystemState(fs, new Path("/"), LOG);

    // Perform the actual load
    new LoadIncrementalHFiles(conf).doBulkLoad(testDir, table);

    // Ensure data shows up
    int expectedRows = NMapInputFormat.getNumMapTasks(conf) * ROWSPERSPLIT;
    assertEquals("LoadIncrementalHFiles should put expected data in table", expectedRows + 1,
      UTIL.countRows(table));
    //
    // // also check to see that we have loaded the WAL into the replication queue
    // fail("Haven't implemented wal loading into repl queue");
  }

  private void runIncrementalPELoad(Configuration conf, HTable table, Path outDir) throws Exception {
    Job job = new Job(conf, "testLocalMRIncrementalLoad");
    job.setWorkingDirectory(UTIL.getDataTestDirOnTestFS("runIncrementalPELoad"));
    job.getConfiguration().setStrings("io.serializations", conf.get("io.serializations"),
      MutationSerialization.class.getName(), ResultSerialization.class.getName(),
      KeyValueSerialization.class.getName());
    TestHFileOutputFormat.setupRandomGeneratorMapper(job);
    HFileOutputFormat.configureIncrementalLoad(job, table);
    WALOutputFormat.updateJobIfEnabled(job, table);
    FileOutputFormat.setOutputPath(job, outDir);

    assertFalse(UTIL.getTestFileSystem().exists(outDir));

    assertEquals(table.getRegionLocations().size(), job.getNumReduceTasks());

    assertTrue(job.waitForCompletion(true));
  }
  
  @Test
  public void testPutReducer() throws Exception{
    fail("Not yet implemented!");
  }
}