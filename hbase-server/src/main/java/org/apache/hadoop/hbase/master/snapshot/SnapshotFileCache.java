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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.util.FSUtils;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

/**
 * Intelligently keep track of all the files for all the snapshots.
 * <p>
 * A cache of files is kept to avoid querying the {@link FileSystem} frequently, and if there is a
 * cache miss the directory modification time is used to ensure that we don't rescan directories
 * that we already have in cache. Further, with periodic refreshes ensure that deleted snapshots are
 * removed from the cache.
 * <p>
 * A {@link PathFilter} must be passed when creating <tt>this</tt> to allow filtering of chilren
 * under the /hbase/.snapshot/[snapshot name] directory, for each snapshot. The filter is only
 * applied the children, not to the children of children. For instance, given the layout:
 * <pre>
 * /hbase/.snapshot/SomeSnapshot/
 *                          .logs/
 *                              server/
 *                                server.1234567
 *                          .regioninfo
 *                          1234567890/
 *                              family/
 *                                  123456789
 * </pre>
 * would only apply a filter to directories: .logs, .regioninfo and 1234567890, not to their
 * children.
 * <p>
 * <tt>this</tt> also considers all running snapshots (those under /hbase/.snapshot/.tmp) as valid
 * snapshots and will attempt to cache files from those snapshots as well.
 * <p>
 * Queries about a given file are thread-safe with respect to multiple queries and cache refreshes.
 */
public class SnapshotFileCache implements Stoppable {
  public interface FilesFilter {
    Collection<String> snapshotFiles(final Path snapshotDir) throws IOException;
  }

  private static final Log LOG = LogFactory.getLog(SnapshotFileCache.class);
  private volatile boolean stop = false;
  private final FileSystem fs;
  private final FilesFilter filesFilter;
  private final Path snapshotDir;
  private final Set<String> cache = new HashSet<String>();
  // helper map so we don't need to rescan each snapshot directory if we know about it
  private final Multimap<String, String> snapshots = ArrayListMultimap.create();
  private final Timer refreshTimer;

  private long lastModifiedTime = Long.MIN_VALUE;

  /**
   * Create a snapshot file cache for all snapshots under the specified [root]/.snapshot on the
   * filesystem.
   * <p>
   * Immediately loads the file cache.
   * @param conf to extract the configured {@link FileSystem} where the snapshots are stored and
   *          hbase root directory
   * @param cacheRefreshPeriod frequency (ms) with which the cache should be refreshed
   * @param refreshThreadName name of the cache refresh thread
   * @param inspectSnapshotFiles Filter to apply to each snapshot to extract the files.
   * @throws IOException if the {@link FileSystem} or root directory cannot be loaded
   */
  public SnapshotFileCache(Configuration conf, long cacheRefreshPeriod, String refreshThreadName,
      FilesFilter inspectSnapshotFiles) throws IOException {
    this(FSUtils.getCurrentFileSystem(conf), FSUtils.getRootDir(conf), 0, cacheRefreshPeriod,
        refreshThreadName, inspectSnapshotFiles);
  }

  /**
   * Create a snapshot file cache for all snapshots under the specified [root]/.snapshot on the
   * filesystem
   * @param fs {@link FileSystem} where the snapshots are stored
   * @param rootDir hbase root directory
   * @param cacheRefreshPeriod frequency (ms) with which the cache should be refreshed
   * @param cacheRefreshDelay amount of time to wait for the cache to be refreshed
   * @param refreshThreadName name of the cache refresh thread
   * @param inspectSnapshotFiles Filter to apply to each snapshot to extract the files.
   */
  public SnapshotFileCache(FileSystem fs, Path rootDir, long cacheRefreshPeriod,
      long cacheRefreshDelay, String refreshThreadName, FilesFilter inspectSnapshotFiles) {
    this.fs = fs;
    this.filesFilter = inspectSnapshotFiles;
    this.snapshotDir = SnapshotDescriptionUtils.getSnapshotDir(rootDir);
    // periodically refresh the file cache to make sure we aren't superfluously saving files.
    this.refreshTimer = new Timer(refreshThreadName, true);
    this.refreshTimer.scheduleAtFixedRate(new RefreshCacheTask(), cacheRefreshDelay,
      cacheRefreshPeriod);
  }

  /**
   * Trigger a cache refresh, even if its before the next cache refresh. Does not affect pending
   * cache refreshes.
   * <p>
   * Blocks until the cache is refreshed.
   * <p>
   * Exposed for TESTING.
   */
  public void triggerCacheRefreshForTesting() {
    LOG.debug("Triggering cache refresh");
    new RefreshCacheTask().run();
    LOG.debug("Current cache:" + cache);
  }

  /**
   * Check to see if the passed file name is contained in any of the snapshots. First checks an
   * in-memory cache of the files to keep. If its not in the cache, then the cache is refreshed and
   * checked again for that file.
   * <p>
   * Note this may lead to periodic false positives for the file being referenced. Periodically, the
   * cache is refreshed even if there are no requests to ensure that the false negatives get removed
   * eventually. However, no false negatives can be found.
   * @param fileName file to check
   * @return <tt>false</tt> if the file is not referenced in any current or running snapshot,
   *         <tt>true</tt> if the file is in the cache.
   * @throws IOException if there is an unexpected error reaching the filesystem.
   */
  // XXX this is inefficient to synchronize on the method, when what we really need to guard against
  // is an illegal access to the cache. Really we could do a mutex-guarded pointer swap on the
  // cache, but that seems overkill at the moment and isn't necessarily a bottleneck.
  public synchronized boolean contains(String fileName) throws IOException {
    if (this.cache.contains(fileName)) return true;

    refreshCache();

    // then check again
    return this.cache.contains(fileName);
  }

  private synchronized void refreshCache() throws IOException {
    // get the status of the snapshots directory
    FileStatus status;
    try {
      status = fs.getFileStatus(snapshotDir);
    } catch (FileNotFoundException e) {
      LOG.warn("Snasphot directory: " + snapshotDir + " doesn't exist");
      return;
    }
    // if the snapshot directory wasn't modified since we last check, we are done
    if (status.getModificationTime() <= lastModifiedTime) return;

    // directory was modified, so we need to reload our cache

    // 1. update the modified time
    this.lastModifiedTime = status.getModificationTime();

    // 2.clear the cache
    this.cache.clear();
    Multimap<String, String> known = ArrayListMultimap.create();

    // 3. check each of the snapshot directories
    FileStatus[] snapshots = FSUtils.listStatus(fs, snapshotDir);
    if (snapshots == null) {
      // remove all the remembered snapshots because we don't have any left
      LOG.debug("No snapshots on-disk, cache empty");
      this.snapshots.clear();
      return;
    }

    // 3.1 iterate through the on-disk snapshots
    for (FileStatus snapshot : snapshots) {
      String name = snapshot.getPath().getName();

      // its the tmp dir
      if (name.equals(SnapshotDescriptionUtils.SNAPSHOT_TMP_DIR)) {
        // only add those files to the cache, but not to the known snapshots
        FileStatus[] running = FSUtils.listStatus(fs, snapshot.getPath());
        if (running == null) continue;
        for (FileStatus run : running) {
          this.cache.addAll(filesFilter.snapshotFiles(run.getPath()));
        }
      } else {
        Collection<String> files = this.snapshots.removeAll(name);
        // 3.1.1if we know about the snapshot already, then keep its files
        if (files.size() <= 0) {
          // otherwise we need to look into the files in the snapshot
          files = filesFilter.snapshotFiles(snapshot.getPath());
        }
        // 3.2 add all the files to cache
        this.cache.addAll(files);
        known.putAll(name, files);
      }
    }

    // 4. set the snapshots we are tracking
    this.snapshots.clear();
    this.snapshots.putAll(known);
  }

  /**
   * Simple helper task that just periodically attempts to refresh the cache
   */
  public class RefreshCacheTask extends TimerTask {
    @Override
    public void run() {
      try {
        SnapshotFileCache.this.refreshCache();
      } catch (IOException e) {
        LOG.warn("Failed to refresh snapshot hfile cache!", e);
      }
    }
  }

  @Override
  public void stop(String why) {
    if (!this.stop) {
      this.stop = true;
      this.refreshTimer.cancel();
    }

  }

  @Override
  public boolean isStopped() {
    return this.stop;
  }
}
