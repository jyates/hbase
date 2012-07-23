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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category(SmallTests.class)
public class TestHFileCleaner {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static ZooKeeperWatcher zkw;

  @BeforeClass
  public static void setupTests() throws Exception {
    zkw = new ZooKeeperWatcher(TEST_UTIL.getConfiguration(), "dummy server", null);
  }

  @Test
  public void testHFileCleaning() throws Exception{
    String prefix = "someHFileThatWouldBeAUUID";
    Configuration conf = TEST_UTIL.getConfiguration();
    // set TTL
    long ttl = 2000;
    conf.setLong(TimeToLiveHFileCleaner.TTL_CONF_KEY, ttl);

    // mock out the server
    Server server = Mockito.mock(Server.class);
    Mockito.when(server.getZooKeeper()).thenReturn(zkw);
    Mockito.when(server.getServerName()).thenReturn(new ServerName("regionserver,60020,000000"));
    Mockito.when(server.getConfiguration()).thenReturn(conf);

    Path archivedHfileDir = new Path(TEST_UTIL.getDataTestDir(),
        HFileArchiveUtil.getConfiguredArchiveDirName(conf));
    FileSystem fs = FileSystem.get(conf);
    HFileCleaner cleaner = new HFileCleaner(1000, server, conf, fs, archivedHfileDir);

    // Create 2 invalid files, 1 "recent" file, 1 very new file and 30 old files
    long now = System.currentTimeMillis();
    fs.delete(archivedHfileDir, true);
    fs.mkdirs(archivedHfileDir);
    // Case 1: 1 invalid file, which would be deleted directly
    fs.createNewFile(new Path(archivedHfileDir, "dfd-dfd"));
    // Case 2: 1 "recent" file, not even deletable for the first log cleaner
    // (TimeToLiveLogCleaner), so we are not going down the chain
    System.out.println("Now is: " + now);
    for (int i = 1; i < 32; i++) {
      // Case 3: old files which would be deletable for the first log cleaner
      // (TimeToLiveHFileCleaner),
      Path fileName = new Path(archivedHfileDir, (prefix + "." + (now - i)));
      fs.createNewFile(fileName);
    }

    // sleep for sometime to get newer modifcation time
    Thread.sleep(ttl);

    // Case 2: 1 newer file, not even deletable for the first log cleaner
    // (TimeToLiveLogCleaner), so we are not going down the chain
    fs.createNewFile(new Path(archivedHfileDir, prefix + "." + (now + 10000)));

    for (FileStatus stat : fs.listStatus(archivedHfileDir)) {
      System.out.println(stat.getPath().toString());
    }

    assertEquals(33, fs.listStatus(archivedHfileDir).length);

    cleaner.chore();

    // We end up a small number - just the one newer one
    assertEquals(1, fs.listStatus(archivedHfileDir).length);

    for (FileStatus file : fs.listStatus(archivedHfileDir)) {
      System.out.println("Kept log files: " + file.getPath().getName());
    }

    cleaner.interrupt();
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

