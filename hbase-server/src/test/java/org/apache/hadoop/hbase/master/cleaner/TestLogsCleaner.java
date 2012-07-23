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
package org.apache.hadoop.hbase.master.cleaner;

import static org.junit.Assert.assertEquals;

import java.net.URLEncoder;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.master.cleaner.LogCleaner;
import org.apache.hadoop.hbase.replication.ReplicationZookeeper;
import org.apache.hadoop.hbase.replication.regionserver.Replication;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category(MediumTests.class)
public class TestLogsCleaner {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static ZooKeeperWatcher zkw;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniZKCluster();
    zkw = new ZooKeeperWatcher(TEST_UTIL.getConfiguration(), "dummy server", null);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
  }

  @Test
  public void testLogCleaning() throws Exception{
    Configuration conf = TEST_UTIL.getConfiguration();
    // set TTL
    long ttl = 2000;
    conf.setLong("hbase.master.logcleaner.ttl", ttl);
    conf.setBoolean(HConstants.REPLICATION_ENABLE_KEY, true);
    Replication.decorateMasterConfiguration(conf);
    // mock out the server
    Server server = Mockito.mock(Server.class);
    Mockito.when(server.getZooKeeper()).thenReturn(zkw);
    Mockito.when(server.getServerName()).thenReturn(new ServerName("regionserver,60020,000000"));
    Mockito.when(server.getConfiguration()).thenReturn(conf);

    ReplicationZookeeper zkHelper =
        new ReplicationZookeeper(server, new AtomicBoolean(true));

    Path oldLogDir = new Path(TEST_UTIL.getDataTestDir(),
        HConstants.HREGION_OLDLOGDIR_NAME);
    String fakeMachineName =
      URLEncoder.encode(server.getServerName().toString(), "UTF8");

    FileSystem fs = FileSystem.get(conf);
    LogCleaner cleaner  = new LogCleaner(1000, server, conf, fs, oldLogDir);

    // Create 2 invalid files, 1 "recent" file, 1 very new file and 30 old files
    long now = System.currentTimeMillis();
    fs.delete(oldLogDir, true);
    fs.mkdirs(oldLogDir);
    // Case 1: 2 invalid files, which would be deleted directly
    fs.createNewFile(new Path(oldLogDir, "a"));
    fs.createNewFile(new Path(oldLogDir, fakeMachineName + "." + "a"));
    // Case 2: 1 "recent" file, not even deletable for the first log cleaner
    // (TimeToLiveLogCleaner), so we are not going down the chain
    System.out.println("Now is: " + now);
    for (int i = 1; i < 31; i++) {
      // Case 3: old files which would be deletable for the first log cleaner
      // (TimeToLiveLogCleaner), and also for the second (ReplicationLogCleaner)
      Path fileName = new Path(oldLogDir, fakeMachineName + "." + (now - i) );
      fs.createNewFile(fileName);
      // Case 4: put 3 old log files in ZK indicating that they are scheduled
      // for replication so these files would pass the first log cleaner
      // (TimeToLiveLogCleaner) but would be rejected by the second
      // (ReplicationLogCleaner)
      if (i % (30/3) == 1) {
        zkHelper.addLogToList(fileName.getName(), fakeMachineName);
        System.out.println("Replication log file: " + fileName);
      }
    }

    // sleep for sometime to get newer modifcation time
    Thread.sleep(ttl);
    fs.createNewFile(new Path(oldLogDir, fakeMachineName + "." + now));

    // Case 2: 1 newer file, not even deletable for the first log cleaner
    // (TimeToLiveLogCleaner), so we are not going down the chain
    fs.createNewFile(new Path(oldLogDir, fakeMachineName + "." + (now + 10000) ));

    for (FileStatus stat : fs.listStatus(oldLogDir)) {
      System.out.println(stat.getPath().toString());
    }

    assertEquals(34, fs.listStatus(oldLogDir).length);

    cleaner.chore();

    // We end up with the current log file, a newer one and the 3 old log
    // files which are scheduled for replication
    assertEquals(5, fs.listStatus(oldLogDir).length);

    for (FileStatus file : fs.listStatus(oldLogDir)) {
      System.out.println("Kept log files: " + file.getPath().getName());
    }
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

