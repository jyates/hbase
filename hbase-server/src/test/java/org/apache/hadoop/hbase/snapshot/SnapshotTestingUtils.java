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
package org.apache.hadoop.hbase.snapshot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;

/**
 * Utilities class for snapshots
 */
public class SnapshotTestingUtils {
  /**
   * Confirm that the snapshot contains references to all the files that should be in the snapshot
   */
  public static void confirmSnapshotValid(SnapshotDescriptor snapshotDescriptor, byte[] tableName,
      byte[] testFamily, Path rootDir, HBaseAdmin admin, FileSystem fs, boolean requireLogs)
      throws IOException {
    Path snapshotDir = SnapshotDescriptor.getCompletedSnapshotDir(snapshotDescriptor, rootDir);
    assertTrue(fs.exists(snapshotDir));
    Path snapshotinfo = new Path(snapshotDir, SnapshotDescriptor.SNAPSHOTINFO_FILE);
    assertTrue(fs.exists(snapshotinfo));
    // check the logs dir
    if (requireLogs) {
      Path logsDir = new Path(snapshotDir, ".logs");
      assertTrue("Logs directory doesn't exist in snapshot", fs.exists(logsDir));
      // make sure we have some logs
      assertTrue(fs.listStatus(logsDir).length > 0);
    }
    // check the table info
    HTableDescriptor desc = FSTableDescriptors.getTableDescriptor(fs, rootDir, tableName);
    HTableDescriptor snapshotDesc = FSTableDescriptors.getTableDescriptor(fs, snapshotDir,
      tableName);
    assertEquals(desc, snapshotDesc);

    // check the region snapshot for all the regions
    List<HRegionInfo> regions = admin.getTableRegions(tableName);
    for (HRegionInfo info : regions) {
      String regionName = info.getEncodedName();
      Path regionDir = new Path(snapshotDir, regionName);
      HRegionInfo snapshotRegionInfo = HRegion.loadDotRegionInfoFileContent(fs, regionDir);
      assertEquals(info, snapshotRegionInfo);
      // check to make sure we have the family
      Path familyDir = new Path(regionDir, Bytes.toString(testFamily));
      assertTrue(fs.exists(familyDir));
      // make sure we have some files references
      assertTrue(fs.listStatus(familyDir).length > 0);
    }
  }
}
