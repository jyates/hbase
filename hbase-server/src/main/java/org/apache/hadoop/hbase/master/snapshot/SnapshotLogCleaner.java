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

import java.io.IOException;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.master.cleaner.BaseLogCleanerDelegate;
import org.apache.hadoop.hbase.util.FSUtils;
import org.jboss.netty.handler.codec.http.HttpHeaders.Names;

/**
 * Implementation of a log cleaner that checks if a log is still used by
 * snapshots of HBase tables.
 */
@InterfaceAudience.Private
public class SnapshotLogCleaner extends BaseLogCleanerDelegate implements
    SnapshotCleanerCacheLoader {
  private static final Log LOG = LogFactory.getLog(SnapshotLogCleaner.class);

  /**
   * Conf key for the frequency to attempt to refresh the cache of hfiles currently used in
   * snapshots (ms)
   */
  static final String HLOG_CACHE_REFRESH_PERIOD_CONF_KEY = "hbase.master.hlogcleaner.plugins.snapshot.period";

  /** Refresh cache, by default, every 5 minutes */
  private static final long DEFAULT_HLOG_CACHE_REFRESH_PERIOD = 300000;

  private SnapshotCleanerUtil cleaner;

  @Override
  public synchronized boolean isFileDeletable(Path filePath) {
    return cleaner.isFileDeletable(filePath);
  }

  @Override
  public void loadFiles(FileSystem fs, FileStatus snapshotDir, Set<String> cache)
      throws IOException {
    // get the logs directory
    FileStatus[] logsDir = fs.listStatus(snapshotDir.getPath());
    // short circuit if directory got removed
    if (logsDir == null || logsDir.length == 0) return;
    for (FileStatus serverLogsDir : logsDir) {

      // if it isn't the log directory, then skip doing a lookup
      if (!serverLogsDir.getPath().getName().equals(HConstants.HREGION_LOGDIR_NAME)) continue;

      // get the logs directory for each server
      FileStatus[] serverLogDirs = fs.listStatus(serverLogsDir.getPath());
      if (serverLogDirs == null || serverLogDirs.length == 0) continue;
      for (FileStatus serverLogs : serverLogDirs) {

        // get the logs for each server
        FileStatus[] logs = FSUtils.listStatus(fs, serverLogs.getPath(), null);
        if (logs == null || logs.length == 0) continue;
        for (FileStatus log : logs) {
          // first we need to strip the time off the filename
          String name = log.getPath().getName();
          String[] parts = name.split("[.]");
          if (parts.length != 3) {
            LOG.debug("Got an hfile in the snapshot that we don't recognize: " + name);
            continue;
          }

          // then prepend "hlog." since archive hlogs get that name
          cache.add("hlog." + parts[1].trim());
        }
      }
    }
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    long cacheRefreshPeriod = conf.getLong(HLOG_CACHE_REFRESH_PERIOD_CONF_KEY,
      DEFAULT_HLOG_CACHE_REFRESH_PERIOD);
    try {
      this.cleaner = new SnapshotCleanerUtil(this, conf, cacheRefreshPeriod,
          "HLog-snapshot_cleaner-cache-refresh-timer");
    } catch (IOException e) {
      LOG.error("Failed to create cleaner util", e);
    }
  }

  @Override
  public void stop(String why) {
    this.cleaner.stop(why);
  }

  @Override
  public boolean isStopped() {
    return this.cleaner.isStopped();
  }
}
