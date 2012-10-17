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
import java.text.SimpleDateFormat;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.HLogLink;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.FSTableDescriptors;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class SnapshotInfo extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(SnapshotInfo.class);

  private FileSystem fs;
  private Path rootDir;

  private HTableDescriptor snapshotTableDesc;
  private SnapshotDescription snapshotDesc;
  private Path snapshotDir;

  @Override
  public int run(String[] args) throws IOException, InterruptedException {
    byte[] snapshotName = null;
    boolean showFiles = false;

    // Process command line args
    for (int i = 0; i < args.length; i++) {
      String cmd = args[i];
      try {
        if (cmd.equals("-snapshot")) {
          snapshotName = Bytes.toBytes(args[++i]);
        } else if (cmd.equals("-files")) {
          showFiles = true;
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
    fs = FileSystem.get(conf);
    rootDir = FSUtils.getRootDir(conf);

    // Load snapshot information
    if (!loadSnapshotInfo(snapshotName)) {
      System.err.println("Snapshot '" + Bytes.toString(snapshotName) + "' not found!");
      return 1;
    }

    printInfo();
    if (showFiles) printFiles();

    return 0;
  }

  private boolean loadSnapshotInfo(final byte[] snapshotName) throws IOException {
    snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
    if (!fs.exists(snapshotDir)) return false;

    snapshotDesc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
    snapshotTableDesc = FSTableDescriptors.getTableDescriptor(fs, snapshotDir);
    return true;
  }

  private void printInfo() {
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    System.out.println("Snapshot Info");
    System.out.println("----------------------------------------");
    System.out.println("   Name: " + snapshotDesc.getName());
    System.out.println("   Type: " + snapshotDesc.getType());
    System.out.println("  Table: " + snapshotDesc.getTable());
    System.out.println("Created: " + df.format(new Date(snapshotDesc.getCreationTime())));
    System.out.println();
  }

  private void printFiles() throws IOException {
    final String table = snapshotDesc.getTable();
    final Configuration conf = getConf();

    System.out.println("Snapshot Files");
    System.out.println("----------------------------------------");

    final AtomicInteger hfilesCount = new AtomicInteger();
    final AtomicInteger logsCount = new AtomicInteger();
    final AtomicLong hfileSize = new AtomicLong();
    final AtomicLong logSize = new AtomicLong();
    SnapshotReferenceUtil.listReferencedFiles(fs, snapshotDir,
      new SnapshotReferenceUtil.FileVisitor() {
        public void storeFile (final String region, final String family, final String hfile)
            throws IOException {
          Path path = new Path(family, HFileLink.createHFileLinkName(table, region, hfile));
          long size = fs.getFileStatus(HFileLink.getReferencedPath(conf, fs, path)).getLen();
          hfileSize.addAndGet(size);
          hfilesCount.addAndGet(1);

          System.out.printf("%8s %s/%s/%s/%s\n",
            StringUtils.humanReadableInt(size), table, region, family, hfile);
        }

        public void recoveredEdits (final String region, final String logfile)
            throws IOException {
          Path path = SnapshotReferenceUtil.getRecoveredEdits(snapshotDir, region, logfile);
          long size = fs.getFileStatus(path).getLen();
          logSize.addAndGet(size);
          logsCount.addAndGet(1);

          System.out.printf("%8s recovered.edits %s on region %s\n",
            StringUtils.humanReadableInt(size), logfile, region);
        }

        public void logFile (final String server, final String logfile)
            throws IOException {
          HLogLink logLink = new HLogLink(conf, server, logfile);
          long size = logLink.getFileStatus(fs).getLen();
          logSize.addAndGet(size);
          logsCount.addAndGet(1);

          System.out.printf("%8s log %s on server %s\n",
            StringUtils.humanReadableInt(size), logfile, server);
        }
    });

    System.out.println();
    System.out.printf("%d HFiles, total size %s\n",
      hfilesCount.get(), StringUtils.humanReadableInt(hfileSize.get()));
    System.out.printf("%d Logs, total size %s\n",
      logsCount.get(), StringUtils.humanReadableInt(logSize.get()));
    System.out.println();
  }

  private void printUsageAndExit() {
    System.err.printf("Usage: bin/hbase %s [options]\n", getClass().getName());
    System.err.println(" where [options] are:");
    System.err.println("  -h|-help                Show this help and exit.");
    System.err.println("  -snapshot NAME          Snapshot to examine.");
    System.err.println("  -files                  Files and logs list.");
    System.err.println();
    System.err.println("Examples:");
    System.err.println("  hbase " + getClass() + " \\");
    System.err.println("    -snapshot MySnapshot");
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
    return ToolRunner.run(HBaseConfiguration.create(), new SnapshotInfo(), args);
  }

  public static void main(String[] args) throws Exception {
     System.exit(innerMain(args));
  }
}
