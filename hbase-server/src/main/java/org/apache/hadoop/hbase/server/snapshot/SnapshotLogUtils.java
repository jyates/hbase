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
package org.apache.hadoop.hbase.server.snapshot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * Utilities for dealing with logs in snapshots
 */
public class SnapshotLogUtils {

  private static final Log LOG = LogFactory.getLog(SnapshotLogUtils.class);

  private SnapshotLogUtils() {
    // private constructor for util class
  }

  /**
   * Get all the log files references by a snapshot
   * @param fs filesystem where the snapshot was taken
   * @param snapshotDir directory containing the snapshot
   * @return list of hlogs referenced by the snapshot
   * @throws IOException if the filesystem cannot be reached
   */
  public static List<FileStatus> getSnapshotHLogs(FileSystem fs, Path snapshotDir)
      throws IOException {
    List<FileStatus> logs = new ArrayList<FileStatus>();
    // get the logs directory
    Path logDir = new Path(snapshotDir, HConstants.HREGION_LOGDIR_NAME);
    // short circuit if directory got removed
    if (!fs.exists(logDir)) return logs;

    // get the servers in the logs
    FileStatus[] servers = FSUtils.listStatus(fs, logDir, new FSUtils.DirFilter(fs));
    if (servers == null || servers.length == 0) return logs;
    for (FileStatus serverLogs : servers) {
      // get the logs for each server
      FileStatus[] logsFiles = FSUtils.listStatus(fs, serverLogs.getPath(), null);
      if (logsFiles == null || logsFiles.length == 0) continue;
      for (FileStatus log : logsFiles) {

        logs.add(log);
      }
    }
    return logs;
  }

  /**
   * Get the real log name (cooresponding to the hlog timestamp) from the full filename (which can
   * be found via {@link #getSnapshotHLogs(FileSystem, Path)})
   * @param fileName full hlog file name
   * @return the hlog name or <tt>null</tt> if the filename isn't valid.
   */
  public static String getHLogName(String fileName) {
    // strip the time off the filename
    String[] parts = fileName.split("[.]");
    if (parts.length < 3) {
      LOG.debug("Got log in the snapshot that we don't recognize: " + fileName);
      return null;
    }
    return parts[parts.length - 2].trim();
  }

  /**
   * Get the oldlogs version of the full filename (which can be found via
   * {@link #getSnapshotHLogs(FileSystem, Path)})
   * @param fileName full hlog name
   * @return name of the file as it would be in the oldlogs dir
   */
  public static String getOldLogsName(String fileName) {
    return "hlog." + getHLogName(fileName);
  }

  /**
   * Get the log directory for a specific snapshot
   * @param snapshotDir directory where the specific snapshot will be store
   * @param serverName name of the parent regionserver for the log files
   * @return path to the log home directory for the archive files.
   */
  public static Path getLogSnapshotDir(Path snapshotDir, String serverName) {
    return new Path(snapshotDir, HLog.getHLogDirectoryName(serverName));
  }
}