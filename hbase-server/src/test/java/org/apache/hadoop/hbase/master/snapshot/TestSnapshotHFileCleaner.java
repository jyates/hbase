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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test that the snapshot hfile cleaner finds hfiles referenced in a snapshot
 */
@Category(SmallTests.class)
public class TestSnapshotHFileCleaner {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  
  @AfterClass
  public static void cleanup() throws IOException{
    Configuration conf = TEST_UTIL.getConfiguration();
    Path rootDir = FSUtils.getRootDir(conf);
    FileSystem fs = FileSystem.get(conf);
    //cleanup
    fs.delete(rootDir, true);
  }
  
  @Test
  public void testFindsSnapshotFilesWhenCleaning() throws IOException{
    Configuration conf = TEST_UTIL.getConfiguration();
    FSUtils.setRootDir(conf, TEST_UTIL.getDataTestDir());
    Path rootDir = FSUtils.getRootDir(conf);
    Path archivedHfileDir = new Path(TEST_UTIL.getDataTestDir(),
        HFileArchiveUtil.getConfiguredArchiveDirName(conf));
    
    FileSystem fs = FileSystem.get(conf);
    SnapshotHFileCleaner cleaner = new SnapshotHFileCleaner();
    cleaner.setConf(conf);

    //write an hfile to the snapshot directory
    byte[] snapshot = Bytes.toBytes("snapshot");
    String table = "table";
    byte[] tableName = Bytes.toBytes(table);
    Path snapshotDir = SnapshotDescriptor.getCompletedSnapshotDir(snapshot, rootDir);
    HRegionInfo mockRegion =new HRegionInfo(tableName);
    Path regionSnapshotDir = new Path(snapshotDir, mockRegion.getEncodedName());
    Path familyDir = new Path(regionSnapshotDir, "family");
    //create a reference to a supposedly valid hfile
    String hfile = "fd1e73e8a96c486090c5cec07b4894c4";
    String reference = hfile+"."+table;
    Path refFile = new Path(familyDir, reference);
    //make sure they are valid
    assertTrue(Reference.isReference(refFile));
    assertEquals(hfile, Reference.getDeferencedHFileName(hfile));
    assertEquals(hfile, Reference.getDeferencedHFileName(reference));
    
    //make sure the reference file exists
    fs.mkdirs(refFile.getParent());
    fs.createNewFile(refFile);
    
    //create the hfile in the archive
    fs.mkdirs(archivedHfileDir);
    fs.createNewFile(new Path(archivedHfileDir, hfile));
    
    //make sure that the file isn't deletable
    assertFalse(cleaner.isFileDeletable(new Path(hfile)));
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}
