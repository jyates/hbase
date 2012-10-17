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

package org.apache.hadoop.hbase.snapshot;

import java.io.IOException;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogUtil;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.FSVisitor;

/**
 * Utility methods for interacting with the snapshot referenced files.
 */
@InterfaceAudience.Private
public final class SnapshotReferenceUtil {
  public interface FileVisitor extends FSVisitor.StoreFileVisitor,
    FSVisitor.RecoveredEditsVisitor, FSVisitor.LogFileVisitor {
  }

  private SnapshotReferenceUtil() {
    // private constructor for utility class
  }

  /**
   * Get the log directory for a specific snapshot
   * @param snapshotDir directory where the specific snapshot will be store
   * @param serverName name of the parent regionserver for the log files
   * @return path to the log home directory for the archive files.
   */
  public static Path getLogsDir(Path snapshotDir, String serverName) {
    return new Path(snapshotDir, HLogUtil.getHLogDirectoryName(serverName));
  }

  /**
   * Get the snapshot recovered.edits root dir
   */
  public static Path getRecoveredEditsDir(Path snapshotDir) {
    return new Path(snapshotDir, '.' + HLog.RECOVERED_EDITS_DIR);
  }

  /**
   * Get the snapshot recovered.edits dir for the specified region
   */
  public static Path getRecoveredEditsDir(Path snapshotDir, String regionName) {
    return new Path(getRecoveredEditsDir(snapshotDir), regionName);
  }

  /**
   * Get the snapshot recovered.edits file
   */
  public static Path getRecoveredEdits(Path snapshotDir, String regionName, String logfile) {
    return new Path(getRecoveredEditsDir(snapshotDir, regionName), logfile);
  }

  /**
   * Iterate over the snapshot store files, restored.edits and logs
   */
  public static void listReferencedFiles(final FileSystem fs, final Path snapshotDir,
      final FileVisitor visitor) throws IOException {
    listStoreFiles(fs, snapshotDir, visitor);
    listRecoveredEdits(fs, snapshotDir, visitor);
    listLogFiles(fs, snapshotDir, visitor);
  }

  /**
   * Iterate over the snapshot store files
   */
  public static void listStoreFiles(final FileSystem fs, final Path snapshotDir,
      final FSVisitor.StoreFileVisitor visitor) throws IOException {
    FSVisitor.tableStoreFiles(fs, snapshotDir, visitor);
  }

  /**
   * Iterate over the snapshot store files in the specified region
   */
  public static void listRegionStoreFiles(final FileSystem fs, final Path regionDir,
      final FSVisitor.StoreFileVisitor visitor) throws IOException {
    FSVisitor.regionStoreFiles(fs, regionDir, visitor);
  }

  /**
   * Iterate over the snapshot recovered.edits
   */
  public static void listRecoveredEdits(final FileSystem fs, final Path snapshotDir,
      final FSVisitor.RecoveredEditsVisitor visitor) throws IOException {
    FSVisitor.tableRecoveredEdits(fs, snapshotDir, visitor);
  }

  /**
   * Iterate over the snapshot recovered.edits and log files
   */
  public static void listLogFiles(final FileSystem fs, final Path snapshotDir,
      final FSVisitor.LogFileVisitor visitor) throws IOException {
    FSVisitor.logFiles(fs, snapshotDir, visitor);
  }

  /**
   * @return the set of the regions contained in the snapshot
   */
  public static Set<String> getSnapshotRegionNames(final FileSystem fs, final Path snapshotDir)
      throws IOException {
    FileStatus[] regionDirs = FSUtils.listStatus(fs, snapshotDir, new FSUtils.RegionDirFilter(fs));
    if (regionDirs == null) return null;

    Set<String> regions = new HashSet<String>();
    for (FileStatus regionDir: regionDirs) {
      regions.add(regionDir.getPath().getName());
    }
    return regions;
  }

  /**
   * Get the list of hfiles for the specified snapshot region.
   * NOTE: The current implementation keep one Reference file per HFile in a region folder.
   *
   * @param snapshotRegionInfo Snapshot region info
   * @throws IOException
   */
  public static Map<String, List<String>> getRegionHFileReferences(final FileSystem fs,
      final Path snapshotRegionDir) throws IOException {
    final Map<String, List<String>> familyFiles = new TreeMap<String, List<String>>();

    SnapshotReferenceUtil.listRegionStoreFiles(fs, snapshotRegionDir,
      new FSVisitor.StoreFileVisitor() {
        public void storeFile (final String region, final String family, final String hfile)
            throws IOException {
          getFamilyFiles(family).add(hfile);
        }

        private List<String> getFamilyFiles(final String family) {
          List<String> hfiles = familyFiles.get(family);
          if (hfiles == null) {
            hfiles = new LinkedList<String>();
            familyFiles.put(family, hfiles);
          }
          return hfiles;
        }
    });

    return familyFiles;
  }

  /**
   * @return Extract the names of hfiles in the specified snaphot
   */
  public static Set<String> getHFileNames(final FileSystem fs, final Path snapshotDir) throws IOException {
    final Set<String> names = new HashSet<String>();
    listStoreFiles(fs, snapshotDir, new FSVisitor.StoreFileVisitor() {
      public void storeFile (final String region, final String family, final String hfile)
          throws IOException {
        names.add(hfile);
      }
    });
    return names;
  }

  /**
   * @return Extract the names of hlogs in the specified snaphot
   */
  public static Set<String> getHLogNames(final FileSystem fs, final Path snapshotDir) throws IOException {
    final Set<String> names = new HashSet<String>();
    listLogFiles(fs, snapshotDir, new FSVisitor.LogFileVisitor() {
      public void logFile (final String server, final String logfile) throws IOException {
        names.add(logfile);
      }
    });
    return names;
  }
}
