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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

/**
 * Helper class for all utilities related to archival/retrieval of HFiles
 */
public class HFileArchiveUtil {
  private static final Log LOG = LogFactory.getLog(HFileArchiveUtil.class);
  public static final String DEFAULT_HFILE_ARCHIVE_DIRECTORY = ".archive";

  private HFileArchiveUtil() {
    // non-external instantiation - util class
  }

  public static Path getStoreArchivePath(Configuration conf, HRegion region, byte [] family){
    return getStoreArchivePath(conf, region.getRegionInfo(), region.getTableDir(), family);
  }

  /**
   * Get the directory to archive a store directory
   * @param conf {@link Configuration} to read for the archive directory name
   * @param region parent region information under which the store currently
   *          lives
   * @param tabledir directory for the table under which the store currently
   *          lives
   * @param family name of the family in the store
   * @return {@link Path} to the directory to archive the given store or
   *         <tt>null</tt> if it should not be archived
   */
  public static Path getStoreArchivePath(Configuration conf, HRegionInfo region, Path tabledir,
      byte[] family) {
    Path tableArchiveDir = getTableArchivePath(conf, tabledir);
    return Store.getStoreHomedir(tableArchiveDir,
      HRegionInfo.encodeRegionName(region.getRegionName()), family);
  }

  /**
   * Get the directory to archive a store directory
   * @param tabledir the original table directory
   * @param regionName encoded name of the region under which the store lives
   * @param family name of the family in the store
   * @return {@link Path} to the directory to archive the given store or
   *         <tt>null</tt> if it should not be archived
   */
  private static Path getStoreArchivePath(Configuration conf, Path tabledir,
      String regionName, byte[] family) {
    // get the archive directory for a table
    Path archiveDir = getTableArchivePath(conf, tabledir);

    // and then the per-store archive directory comes from that (final
    // destination of archived hfiles)
    return Store.getStoreHomedir(archiveDir, regionName, family);
  }

  /**
   * Get the archive directory for a given region under the specified table
   * @param conf {@link Configuration} to read the archive directory from
   * @param tabledir the original table directory
   * @param regiondir the path to the region directory
   * @return {@link Path} to the directory to archive the given region, or
   *         <tt>null</tt> if it should not be archived
   */
  public static Path getRegionArchiveDir(Configuration conf, Path tabledir, Path regiondir) {
    // get the archive directory for a table
    Path archiveDir = getTableArchivePath(conf, tabledir);
    // if the monitor isn't archiving that table, then we don't specify an
    // archive directory
    if (archiveDir == null) return null;

    // then add on the region path under the archive
    String encodedRegionName = regiondir.getName();
    return HRegion.getRegionDir(archiveDir, encodedRegionName);
  }

  /**
   * Get the path to the table archive directory based on the configured archive
   * directory.
   * <p>
   * Assumed that the table should already be archived.
   * @param conf {@link Configuration} to read the archive property from
   * @param tabledir directory of the table to be archived.
   * @return {@link Path} to the archive directory for the table
   */
  public static Path getTableArchivePath(Configuration conf, Path tabledir) {
    return getTableArchivePath(getConfiguredArchiveDir(conf), tabledir);
  }

  private static Path getTableArchivePath(String archivedir, Path tabledir) {
    Path root = tabledir.getParent();
    // now build the archive directory path
    // first the top-level archive directory
    // generally "/hbase/.archive/[table]
    return new Path(new Path(root, archivedir), tabledir.getName());
  }

  /**
   * Get the archive directory as per the configuration
   * @param conf {@link Configuration} to read the archive directory from (can
   *          be null, in which case you get the default value).
   * @return the configured archived directory or the default specified by
   *         {@value HFileArchiveUtil#DEFAULT_HFILE_ARCHIVE_DIRECTORY}
   */
  public static String getConfiguredArchiveDir(Configuration conf) {
    return conf == null ? HFileArchiveUtil.DEFAULT_HFILE_ARCHIVE_DIRECTORY : conf.get(
      HConstants.HFILE_ARCHIVE_DIRECTORY, HFileArchiveUtil.DEFAULT_HFILE_ARCHIVE_DIRECTORY);
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

  /**
   * Get the zookeeper node associated with archiving the given table
   * @param zkw watcher for the zk cluster
   * @param table name of the table to check
   * @return znode for the table's archive status
   */
  public static String getTableNode(ZooKeeperWatcher zkw, String table) {
    return ZKUtil.joinZNode(zkw.archiveHFileZNode, table);
  }
}
