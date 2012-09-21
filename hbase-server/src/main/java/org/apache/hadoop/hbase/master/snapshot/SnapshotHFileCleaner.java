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
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.master.cleaner.BaseHFileCleanerDelegate;

/**
 * Implementation of a log cleaner that checks if a hfile is still used by snapshots of HBase
 * tables.
 */
@InterfaceAudience.Private
public class SnapshotHFileCleaner extends BaseHFileCleanerDelegate implements
    SnapshotCleanerCacheLoader {
  private static final Log LOG = LogFactory.getLog(SnapshotHFileCleaner.class);

  /**
   * Conf key for the frequency to attempt to refresh the cache of hfiles currently used in
   * snapshots (ms)
   */
  static final String HFILE_CACHE_REFRESH_PERIOD_CONF_KEY = "hbase.master.hfilecleaner.plugins.snapshot.period";

  /** Refresh cache, by default, every 5 minutes */
  private static final long DEFAULT_HFILE_CACHE_REFRESH_PERIOD = 300000;

  private SnapshotCleanerChoreUtil cleaner;

  @Override
  public synchronized boolean isFileDeletable(Path filePath) {
    return cleaner.isFileDeletable(filePath);
  }

  @Override
  public void loadFiles(FileSystem fs, FileStatus snapshot, final Set<String> cache)
      throws IOException {
    FileStatus[] hfiles = SnapshotCleanerChoreUtil.listHFiles(fs, snapshot.getPath());
    if (hfiles == null || hfiles.length == 0) {
      LOG.debug("No hfiles found in snapshot:" + snapshot.getPath());
      return;
    }
    // add all the hfiles for the snapshot
    for (FileStatus file : hfiles) {
      try {
        String referenceName = file.getPath().getName();
        String hfileName = Reference.getDeferencedHFileName(referenceName);
        LOG.debug("Adding hfile:" + hfileName + " to cleaner cache.");
        cache.add(hfileName);
      } catch (IllegalArgumentException e) {
        LOG.error(file.getPath() + " is not a valid hfile or reference file. Ignoring!");
      }
    }
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    try {
      long cacheRefreshPeriod = conf.getLong(HFILE_CACHE_REFRESH_PERIOD_CONF_KEY,
        DEFAULT_HFILE_CACHE_REFRESH_PERIOD);
      cleaner = new SnapshotCleanerChoreUtil(this, conf, cacheRefreshPeriod,
          "HFile-snapshot_cleaner-cache-refresh-timer");
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
