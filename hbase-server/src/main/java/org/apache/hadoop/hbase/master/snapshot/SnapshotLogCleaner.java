/**
 * Copyright 2010 The Apache Software Foundation
 *
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
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.master.LogCleanerDelegate;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * Implementation of a log cleaner that checks if a log is still used by
 * snapshots of HBase tables.
 */
public class SnapshotLogCleaner implements LogCleanerDelegate {
  private static final Log LOG = LogFactory.getLog(SnapshotLogCleaner.class);

  private Configuration conf;
  private FileSystem fs;
  private Path rootDir;
  private Set<String> hlogs = new HashSet<String>();
  private volatile boolean stopped = false;

  public SnapshotLogCleaner() {}

  @Override
  public synchronized boolean isLogDeletable(Path filePath) {
    String log = filePath.getName();
    if (this.hlogs.contains(log)) {
      return false;
    }

    // if we don't have it in the cache, the cache may be empty, so we need to
    // refresh it and then check again
    try {
      refreshHLogsCache();
    } catch (IOException e) {
      LOG.error("Couldn't refresh HLog cache, by default not going to delete log file.");
      return false;
    }
    //if we still don't have the log, then you can delete it
    return !this.hlogs.contains(log);
  }

  /**
   * Refresh the HLogs cache to get the current logs which are used by snapshots
   * under snapshot directory
   * <p>
   * This is run async from the master so its ok if it takes a little longer in
   * the scanning. We could add file references in META, but that takes a bit
   * longer when taking the snapshot, so we trade-off to take some more time
   * here.
   * @throws IOException if we can't access the file system properly
   */
  public synchronized void refreshHLogsCache() throws IOException {
    this.hlogs.clear();
    
    Path snapshotRoot = SnapshotDescriptor.getSnapshotRootDir(rootDir);
    FileStatus[] snapshots = fs.listStatus(snapshotRoot);
    if(snapshots == null)
      return;
    try{
      for(FileStatus snapshot: snapshots){
        if(stopped)
          throw new IOException("Stopped! Cannot read any more files.");
        if(snapshot.isDir()){
          // add all the log files for the snapshot
          addLogsForDirectory(snapshot.getPath());
        }
      }
    }catch (IOException e) {
      LOG.warn("Failed to refresh hlogs cache for snapshots!", e);
      throw e;
    }
  }

  /*
   * Add all the log files in the .logs directory under the passed in snapshot
   * directory.
   */
  private void addLogsForDirectory(Path snapshotDir) throws IOException {
    FileStatus [] files = fs.listStatus(snapshotDir);
    for(FileStatus file :files){
      if(stopped)
        throw new IOException("Stopped! Cannot read any more files.");
      //if its the log directory, then add the logs below it
      if(file.getPath().getName().equals(HConstants.HREGION_LOGDIR_NAME)){
        for(FileStatus log: fs.listStatus(file.getPath())){
          this.hlogs.add(log.getPath().getName());
        }
      }
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    try {
      this.fs = FileSystem.get(this.conf);
      this.rootDir = FSUtils.getRootDir(this.conf);

      // initialize the cache
      refreshHLogsCache();
    } catch (IOException e) {
      LOG.error(e);
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
