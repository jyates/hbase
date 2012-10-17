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

package org.apache.hadoop.hbase.snapshot.tool;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.restore.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.FSTableDescriptors;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class RestoreSnapshot extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(RestoreSnapshot.class);

  private HBaseAdmin admin;
  private FileSystem fs;
  private Path rootDir;

  private SnapshotDescription snapshotDesc;
  private Path snapshotDir;

  private HTableDescriptor snapshotTableDesc;
  private byte[] tableName;
  private Path tableDir;

  @Override
  public int run(String[] args) throws IOException, InterruptedException {
    byte[] takeSnapshotName = null;
    byte[] snapshotName = null;
    boolean noWALs = false;

    // Process command line args
    for (int i = 0; i < args.length; i++) {
      String cmd = args[i];
      try {
        if (cmd.equals("-table")) {
          tableName = Bytes.toBytes(args[++i]);
        } else if (cmd.equals("-restore")) {
          snapshotName = Bytes.toBytes(args[++i]);
        } else if (cmd.equals("-take-snapshot")) {
          takeSnapshotName = Bytes.toBytes(args[++i]);
        } else if (cmd.equals("-no-wal-data")) {
          noWALs = true;
        } else if (cmd.equals("-h") || cmd.equals("--help")) {
          printUsageAndExit();
        } else {
          System.err.println("UNEXPECTED: " + cmd);
          printUsageAndExit();
        }
      } catch (Exception e) {
        printUsageAndExit();
      }
    }

    if (snapshotName == null) {
      System.err.println("Missing snapshot name!");
      printUsageAndExit();
      return 1;
    }

    Configuration conf = getConf();
    admin = new HBaseAdmin(conf);
    fs = FileSystem.get(conf);
    rootDir = FSUtils.getRootDir(conf);

    // Load snapshot information
    if (!loadSnapshotInfo(snapshotName)) {
      System.err.println("Snapshot '" + Bytes.toString(snapshotName) + "' not found!");
      return 1;
    }

    // If No table name is specified, do the restore on the original table.
    if (tableName == null) {
      LOG.info("Table not specified using snapshot table=" + snapshotDesc.getTable());
      tableName = Bytes.toBytes(snapshotDesc.getTable());
    }

    if (admin.tableExists(tableName)) {
      // Table must be disabled
      if (!admin.isTableDisabled(tableName)) {
        System.err.println("Table '"+ Bytes.toString(tableName) +"' must be disabled to restore.");
        System.err.println("Disable the table or restore the table to a new location instead.");
        return 1;
      }
    } else {
      takeSnapshotName = null;
    }

    tableDir = HTableDescriptor.getTableDir(rootDir, tableName);

    // Take a snapshot before restore.
    if (takeSnapshotName != null) {
      admin.snapshot(takeSnapshotName, tableName);
    }

    // Restore snapshot
    CatalogTracker catalogTracker = new CatalogTracker(conf);
    catalogTracker.start();
    try {
      restoreSnapshot(conf, catalogTracker, noWALs);
      return 0;
    } catch (IOException e) {
      System.err.println("Restore snapshot failed: " + e);
      e.printStackTrace();

      if (takeSnapshotName != null) {
        // Try to restore to the snapshot taken
        loadSnapshotInfo(takeSnapshotName);
        restoreSnapshot(conf, catalogTracker, false);
      }
    } finally {
      catalogTracker.stop();
    }

    return 1;
  }

  private boolean loadSnapshotInfo(final byte[] snapshotName) throws IOException {
    snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
    if (!fs.exists(snapshotDir)) return false;

    snapshotDesc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
    snapshotTableDesc = FSTableDescriptors.getTableDescriptor(fs, snapshotDir);
    return true;
  }

  private void restoreSnapshot(final Configuration conf, final CatalogTracker catalogTracker,
      final boolean noWALs) throws IOException {
    HTableDescriptor tableDesc;

    // If table doesn't exists clone from snapshot
    if (!admin.tableExists(tableName)) {
      LOG.info("Table '" + Bytes.toString(tableName) + "' doesn't exists. " +
               "Clone from snapshot " + snapshotDesc.getName());
      tableDesc = RestoreSnapshotHelper.cloneTableSchema(admin, snapshotTableDesc, tableName);
      admin.disableTable(tableName);
    } else {
      LOG.info("Restore Table '" + Bytes.toString(tableName) + "' " +
               "from snapshot " + snapshotDesc.getName());
      tableDesc = RestoreSnapshotHelper.restoreTableSchema(admin, snapshotTableDesc, tableName);
    }

    LOG.info("Restore snapshot="+ snapshotDesc.getName() +" as table="+ tableDesc);
    RestoreSnapshotHelper restoreHelper = new RestoreSnapshotHelper(conf, fs,
                      catalogTracker, snapshotDesc, snapshotDir, tableDesc, tableDir);
    restoreHelper.restore(noWALs);

    admin.enableTable(tableName);
  }

  private void printUsageAndExit() {
    System.err.printf("Usage: bin/hbase %s [options]\n", getClass().getName());
    System.err.println(" where [options] are:");
    System.err.println("  -h|-help                Show this help and exit.");
    System.err.println("  -restore NAME           Snapshot to restore.");
    System.err.println("  -take-snapshot NAME     Take a snapshot before restore.");
    System.err.println("  -table NAME             Restore snapshot on specified table name.");
    System.err.println("  -no-wal-data            Only restore flushed data.");
    System.err.println();
    System.err.println("Examples:");
    System.err.println("  hbase " + getClass() + " \\");
    System.err.println("    -restore MySnapshot --table NewTableName");
    System.exit(1);
  }

  /**
   * The guts of the {@link #main} method.
   * Call this method to avoid the {@link #main(String[])} System.exit.
   * @param args
   * @return errCode
   * @throws Exception
   */
  static int innerMain(final String [] args) throws Exception {
    return ToolRunner.run(HBaseConfiguration.create(), new RestoreSnapshot(), args);
  }

  public static void main(String[] args) throws Exception {
     System.exit(innerMain(args));
  }
}
