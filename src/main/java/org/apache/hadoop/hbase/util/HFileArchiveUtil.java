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
package org.apache.hadoop.hbase.util;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.HFileArchiveMonitor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

import com.google.common.base.Preconditions;

/**
 * Helper class for all utilities related to archival/retrieval of HFiles
 */
public class HFileArchiveUtil {
  private static final Log LOG = LogFactory.getLog(HFileArchiveUtil.class);

  private HFileArchiveUtil() {
    // non-external instantiation - util class
  }

  /**
   * Attempt to archive the passed in file to the store archive directory.
   * <p>
   * If the same file already exists in the archive, it is moved to a
   * timestamped directory under the archive directory and the new file is put
   * in its place.
   * @param fs FileSystem on which to move files
   * @param storeArchiveDirectory {@link Path} to the directory that stores the
   *          archives of the hfiles
   * @param currentFile {@link Path} to the original HFile that will be archived
   * @param archiveStartTime
   * @return <tt>true</tt> if the file is successfully archived. <tt>false</tt>
   *         if there was a problem, but the operation still completed.
   * @throws IOException on failure to complete {@link FileSystem} operations.
   */
  public static boolean resolveAndArchiveFile(FileSystem fs,
      Path storeArchiveDirectory, Path currentFile, String archiveStartTime)
      throws IOException {
    // get the eventual path of the store file as is will be in the archive
    String filename = currentFile.getName();
    Path archiveFile = new Path(storeArchiveDirectory, filename);

    boolean success = true;
    // if the file already exists in the archive, move that one to a
    // timestamped backup
    if (fs.exists(archiveFile)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("File:"+ archiveFile
            + " already exists in archive, moving to timestamped backup and overwriting current.");
      }
      Path backupDir = new Path(storeArchiveDirectory, archiveStartTime);

      // make the backup dir unless we have already
      if (!fs.exists(backupDir)) {
        if (!fs.mkdirs(backupDir)) {
          LOG.warn("Failed to create backup archive: " + backupDir
              + ", deleting existing file to copy in new file.");
          // delete the existing file so we can copy in the new file - this
          // should almost never happen, but makes sure we always have the
          // latest copy
          fs.delete(archiveFile, true);
          success = false;
        }
      }
      // if we can in fact backup the existing archive files
      // otherwise, the previous been deleted when we failed to make the backup
      // directory to store it
      if (fs.exists(backupDir)) {
        // move the archived file into the backup directory
        Path backedupArchivedFile = new Path(backupDir, filename);
        if (!fs.rename(archiveFile, backedupArchivedFile)) {
          success = false;
          LOG.warn("Failed to backup archived file: " + currentFile);
          if (fs.exists(archiveFile)) {
            LOG.warn("Deleting existing archive file: " + archiveFile
                + ", in favor of newer version.");
            if (!fs.delete(archiveFile, true)) {
              LOG.warn("Failed to archive " + currentFile
                  + ", because could not backup existing file.");
              return false;
            }
          }
        } else if (LOG.isDebugEnabled()) {
          LOG.debug("Backed up archive file from: " + archiveFile + ", to: "
              + backedupArchivedFile);
        }
      }
    }

    // now that we have cleared out any existing archive file, move the
    // current file into the new directory
    if (!fs.rename(currentFile, archiveFile)) {
      LOG.warn("Failed to archive file:" + currentFile);
      success = false;
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("Finished archiving file from: " + currentFile + ", to: "
          + archiveFile);
    }
    return success;
  }

  /**
   * Get the directory to archive a store directory
   * @param monitor Monitor if a table should be archived
   * @param tabledir the original table directory
   * @param regionName encoded name of the region under which the store lives
   * @param family name of the family in the store
   * @return {@link Path} to the directory to archive the given store or
   *         <tt>null</tt> if it should not be archived
   */
  public static Path getStoreArchivePath(HFileArchiveMonitor monitor,
      Path tabledir, String regionName, byte[] family) {
    // get the archive directory for a table
    Path archiveDir = getTableArchivePath(monitor, tabledir);
    if (archiveDir == null) return null;

    // and then the per-store archive directory comes from that (final
    // destination of archived hfiles)
    return Store.getStoreHomedir(archiveDir, regionName, family);
  }

  /**
   * Get the archive directory for a given region under the specified table
   * @param monitor Monitor if a table should be archived
   * @param tabledir the original table directory
   * @param regiondir the path to the region directory
   * @return {@link Path} to the directory to archive the given region, or
   *         <tt>null</tt> if it should not be archived
   */
  public static Path getRegionArchiveDir(HFileArchiveMonitor monitor,
      Path tabledir, Path regiondir) {
    // get the archive directory for a table
    Path archiveDir = getTableArchivePath(monitor, tabledir);
    if (archiveDir == null) return null;

    // then add on the region path under the archive
    String encodedRegionName = regiondir.getName();
    return HRegion.getRegionDir(archiveDir, encodedRegionName);
  }

  public static Path getTableArchivePath(HFileArchiveMonitor monitor,
      Path tabledir) {
    Preconditions.checkNotNull(monitor,
      "Archive manager must not be null when building store archive path");
    String table = tabledir.getName();
    // get the name of the archive directory for the table
    String backupDir = monitor.getBackupDirectory(table);

    if (backupDir == null) return null;
    Path root = tabledir.getParent();

    // now build the archive directory path
    // first the top-level archive directory
    // generally "/hbase/.archive/[table]
    return new Path(new Path(root, backupDir), tabledir.getName());
  }

  /**
   * Get the zookeeper node associated with archiving the given table
   * @param zkw watcher for the zk cluster
   * @param table name of the table to check
   * @return znode for the table's archive status
   */
  public static String getTableNode(ZooKeeperWatcher zkw, byte[] table) {
    return ZKUtil.joinZNode(zkw.archiveHFileZNode, Bytes.toString(table));
  }
}
