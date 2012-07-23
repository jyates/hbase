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
package org.apache.hadoop.hbase.regionserver.snapshot;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;
import org.apache.hadoop.hbase.snapshot.monitor.SnapshotErrorMonitor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * Utility class for region-specific snapshot functionality
 */
public class RegionSnapshotUtils {

  private static final Log LOG = LogFactory.getLog(RegionSnapshotUtils.class);

  /**
   * Create a reference file for <code>source file</code> under the passed
   * <code>destination directory</code>.
   * <p>
   * NOTE: Will reference the entire file
   * @param fs FileSystem
   * @param conf {@link Configuration} for the creating parent - used for file manipulations
   * @param srcFile file to be referred
   * @param dstDir directory under which reference file is created
   * @return path to the reference file
   * @throws IOException if creating reference file fails
   */
  public static Path createReference(final FileSystem fs, final Configuration conf,
      final Path srcFile, Path dstDir) throws IOException {
    LOG.debug("Creating reference for:" + srcFile + " in directory:" + dstDir);
    Path referenceFile = null;
    // copy the file directly if it is already a reference file
    if (Reference.checkReference(srcFile)) {
      referenceFile = new Path(dstDir, srcFile.getName());
      FileUtil.copy(fs, srcFile, fs, referenceFile, false, conf);
    } else {
      LOG.debug("Creating new reference file for: " + srcFile);
      referenceFile = createReferenceFile(fs, srcFile, dstDir);
      LOG.debug("Created reference file.");
    }
    return referenceFile;
  }

  private static Path createReferenceFile(final FileSystem fs, final Path srcFile, Path dstDir)
      throws IOException {
    // A reference to the entire store file.
    Reference r = Reference.createWholeFileReference();
    LOG.debug("Created reference object.");
    String parentTableName = srcFile.getParent().getParent().getParent().getName();
    // Write reference with same file id only with the other table name+ // as
    // suffix.
    Path p = new Path(dstDir, srcFile.getName() + "." + parentTableName);
    LOG.debug("Got final name:" + p);
    return r.write(fs, p);
  }

  /**
   * Get the per-region snapshot description location.
   * <p>
   * Under the per-snapshot directory, specific files per-region are kept in a similar layout as per
   * the current directory layout.
   * @param desc description of the snapshot
   * @param rootDir root directory for the hbase installation
   * @param regionName encoded name of the region (see {@link HRegionInfo#encodeRegionName(byte[])})
   * @return path to the per-region directory for the snapshot
   */
  public static Path getRegionSnaphshotDirectory(SnapshotDescriptor desc, Path rootDir,
      String regionName) {
    Path snapshotDir = SnapshotDescriptor.getWorkingSnapshotDir(desc, rootDir);
    return HRegion.getRegionDir(snapshotDir, regionName);
  }

  /**
   * Get the home directory for store-level snapshot files.
   * <p>
   * Specific files per store are kept in a similar layout as per the current directory layout.
   * @param regionDir directory for the parent region
   * @param family name of the store to snapshot
   * @return path to the snapshot home directory for the store/family
   */
  public static Path getStoreSnapshotDirectory(Path regionDir, String family) {
    return getStoreSnapshotDirectory(regionDir, Bytes.toBytes(family));
  }

  /**
   * Get the home directory for store-level snapshot files.
   * <p>
   * Specific files per store are kept in a similar layout as per the current directory layout.
   * @param regionDir directory for the parent region
   * @param family family for the store to snapshot
   * @return path to the snapshot home directory for the store/family
   */
  public static Path getStoreSnapshotDirectory(Path regionDir, byte[] family) {
    return Store.getStoreHomedir(regionDir, family);
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

  /**
   * Get all the hfiles in the region, under the passed set of families
   * @param families all the family directories under the region
   * @param fs filesystem where the families live
   * @param fileFilter filter to only include files
   * @return collection of all the hfiles under all the passed in families (non-null)
   * @throws IOException on unexecpted error reading the FS
   */
  public static Collection<FileStatus> getHFilesInRegion(FileStatus[] families, FileSystem fs,
      PathFilter fileFilter) throws IOException {
    Set<FileStatus> files = new TreeSet<FileStatus>();
    for (FileStatus family : families) {
      // get all the hfiles in the family
      FileStatus[] hfiles = FSUtils.listStatus(fs, family.getPath(), fileFilter);
      // if no hfiles, then we are done with this family
      if (hfiles == null || hfiles.length == 0) continue;
      files.addAll(Arrays.asList(hfiles));
    }
    return files;
  }

  /**
   * Wait for latch to count to zero, ignoring any spurious wake-ups, but waking periodically to
   * check for errors
   * @param latch latch to wait on
   * @throws SnapshotCreationException if the snapshot was failed while waiting
   */
  public static void waitForLatch(CountDownLatch latch, String latchType, long wakeFrequency,
      SnapshotErrorMonitor monitor)
      throws SnapshotCreationException {
    do {
      // first check for error, and if none is found then wait
      monitor.failOnError();
      try {
        LOG.debug("Waiting for snapshot " + latchType + " latch.");
        latch.await(wakeFrequency, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        LOG.debug("Wait for latch interrupted, done:" + (latch.getCount() == 0));
      }
    } while (latch.getCount() > 0);
  }
}
