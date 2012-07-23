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
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.BaseConfigurable;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.master.cleaner.FileCleanerDelegate;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.FSUtils.DirFilter;

/**
 * Utility class to help clean up parts of a snapshot from the archive directories.
 */
@InterfaceAudience.Private
public class SnapshotCleanerUtil extends BaseConfigurable implements Stoppable, FileCleanerDelegate {

  private static final Log LOG = LogFactory.getLog(SnapshotCleanerUtil.class);
  private Timer refreshTimer;
  private volatile boolean stopped = false;
  private Set<String> fileNameCache = new TreeSet<String>();
  private final FileSystem fs;
  private final Path rootDir;
  private final DirFilter dirFilter;
  private final SnapshotCleanerCacheLoader parent;

  public SnapshotCleanerUtil(SnapshotCleanerCacheLoader parent, Configuration conf,
      long cacheRefreshPeriod, String refreshThreadName)
      throws IOException {
    this.parent = parent;
    super.setConf(conf);
    this.fs = FSUtils.getCurrentFileSystem(this.getConf());
    this.rootDir = FSUtils.getRootDir(this.getConf());
    this.dirFilter = new FSUtils.DirFilter(fs);
    // periodically refresh the hfile cache to make sure we aren't superfluously saving hfiles.
    this.refreshTimer = new Timer(refreshThreadName, true);
    this.refreshTimer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        try {
          SnapshotCleanerUtil.this.refreshCache();
        } catch (IOException e) {
          LOG.warn("Failed to refresh snapshot hfile cache!");
        }
      }
    }, 0, cacheRefreshPeriod);
      
}

  @Override
  public synchronized boolean isFileDeletable(Path file) {
    LOG.debug("Checking to see if:" + file + " is deletable");
    String fileName = file.getName();
    if (this.fileNameCache.contains(fileName)) {
      return false;
    }

    // if we don't have it in the cache, the cache may be empty, so we need to
    // refresh it and then check again
    try {
      refreshCache();
    } catch (IOException e) {
      LOG.error("Couldn't refresh HLog cache, by default not going to delete log file.");
      return false;
    }
    // if we still don't have the log, then you can delete it
    return !this.fileNameCache.contains(fileName);
  }

  /**
   * 
   */
  private synchronized void refreshCache() throws IOException {
    LOG.debug("Refreshing file cache.");
    this.fileNameCache.clear();
    
    Path snapshotRoot = SnapshotDescriptor.getSnapshotRootDir(rootDir);
    FileStatus[] snapshots = FSUtils.listStatus(fs, snapshotRoot, dirFilter);
    if(snapshots == null)
      return;
    try{
      for(FileStatus snapshot: snapshots){
        if(stopped)
          throw new IOException("Stopped! Cannot read any more files.");
        parent.loadFiles(this.fs, snapshot, this.fileNameCache);
      }
    } catch (IOException e) {
      LOG.warn("Failed to refresh hlogs cache for snapshots!", e);
      throw e;
    }
  }

@Override
public void stop(String why) {
  this.stopped = true;
}

@Override
public boolean isStopped() {
  return this.stopped;
}
}
