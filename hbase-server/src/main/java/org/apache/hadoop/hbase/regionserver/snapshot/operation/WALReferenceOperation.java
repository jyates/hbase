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
package org.apache.hadoop.hbase.regionserver.snapshot.operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.snapshot.RegionSnapshotUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;
import org.apache.hadoop.hbase.snapshot.monitor.SnapshotFailureListener;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * Operation to increment references to all the wals necessary for a given snapshot
 */
public class WALReferenceOperation extends SnapshotOperation {
  private static final Log LOG = LogFactory.getLog(WALReferenceOperation.class);
  // XXX does this need to be HasThread?
  private final List<Path> files;
  private final FileSystem fs;
  private final Configuration conf;
  private final String serverName;
  private boolean done;

  public WALReferenceOperation(SnapshotDescriptor snapshot,
      SnapshotFailureListener failureListener, final Path logDir, final Configuration conf,
      final FileSystem fs, String serverName) throws IOException {
    super(snapshot, failureListener);
    this.fs = fs;
    this.conf = conf;
    this.serverName = serverName;
    // get all the current logs - they all may hold info for this table
    FileStatus[] logFiles = fs.listStatus(logDir);
    // quick exit if no log files
    if (logFiles == null) {
      done = true;
      this.files = null;
      return;
    }
    done = false;
    this.files = new ArrayList<Path>(logFiles.length);
    for (FileStatus file : logFiles) {
      if (!file.isDir()) files.add(file.getPath());
    }
  }

  @Override
  public void run() {
    if (done) {
      LOG.debug("No HLogs to add to snapshot, done!");
      return;
    }
    // Iterate through each of the log files and add a reference to it.
    if (LOG.isDebugEnabled()) LOG.debug("Adding references for WAL files:" + this.files);
    for (Path file : files) {
      if (checkForError()) {
        LOG.error("Could not complete adding WAL files to snapshot "
            + "because received nofification that snapshot failed.");
        return;
      }

      try {
        // make sure file exists and hasn't be moved to oldlogs
        if (!fs.exists(file)) {
          // TODO - switch to using MonitoredTask
          continue;
        }
        // add the reference to the file
        // 0. Build a reference path based on the file name
        // get the current snapshot directory
        Path rootDir = FSUtils.getRootDir(conf);
        Path snapshotDir = SnapshotDescriptor.getWorkingSnapshotDir(this.snapshot, rootDir);
        Path snapshotLogDir = RegionSnapshotUtils.getLogSnapshotDir(snapshotDir, serverName);
        // actually store the reference on disk (small file)
        RegionSnapshotUtils.createReference(fs, conf, file, snapshotLogDir);
        // TODO - switch to using MonitoredTask
        LOG.debug("Completed WAL referencing for: " + file);
      } catch (IOException e) {
        snapshotFailure("Failed to update reference in META for log file:" + file, e);
        return;
      }
    }
    LOG.debug("Completed WAL referencing for ALL files");
  }
}