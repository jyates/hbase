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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionListener;
import org.apache.hadoop.hbase.server.errorhandling.OperationAttemptTimer;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.exception.CorruptedSnapshotException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * Utilities for useful when taking a snapshot
 */
public class TakeSnapshotUtils {

  private static final Log LOG = LogFactory.getLog(TakeSnapshotUtils.class);

  private TakeSnapshotUtils() {
    // private constructor for util class
  }

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
  public static Path getRegionSnaphshotDirectory(SnapshotDescription desc, Path rootDir,
      String regionName) {
    Path snapshotDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(desc, rootDir);
    return HRegion.getRegionDir(snapshotDir, regionName);
  }

  /**
   * Get the home directory for store-level snapshot files.
   * <p>
   * Specific files per store are kept in a similar layout as per the current directory layout.
   * @param regionDir snapshot directory for the parent region, <b>not</b> the standard region
   *          directory. See {@link #getRegionSnaphshotDirectory(SnapshotDescription, Path, String)}
   * @param family name of the store to snapshot
   * @return path to the snapshot home directory for the store/family
   */
  public static Path getStoreSnapshotDirectory(Path regionDir, String family) {
    return HStore.getStoreHomedir(regionDir, Bytes.toBytes(family));
  }

  /**
   * Get the snapshot directory for each family to be added to the the snapshot
   * @param snapshot description of the snapshot being take
   * @param rootDir root hbase directory where the table is stored
   * @param name name of the region directory
   * @param families families to be added (can be null)
   * @return paths to the snapshot directory for each family, in the same order as the families
   *         passed in
   */
  public static List<Path> getFamilySnapshotDirectories(SnapshotDescription snapshot, Path rootDir,
      String name, FileStatus[] families) {
    if (families == null || families.length == 0) return Collections.emptyList();

    List<Path> familyDirs = new ArrayList<Path>(families.length);
    Path snapshotRegionDir = TakeSnapshotUtils.getRegionSnaphshotDirectory(snapshot, rootDir, name);
    for (FileStatus family : families) {
      // build the reference directory name
      familyDirs.add(TakeSnapshotUtils.getStoreSnapshotDirectory(snapshotRegionDir, family
          .getPath().getName()));
    }
    return familyDirs;
  }

  /**
   * Create a snapshot timer for the master which notifies the monitor when an error occurs
   * @param snapshot snapshot to monitor
   * @param conf configuration to use when getting the max snapshot life
   * @param monitor monitor to notify when the snapshot life expires
   * @return the timer to use update to signal the start and end of the snapshot
   */
  @SuppressWarnings("rawtypes")
  public static OperationAttemptTimer getMasterTimerAndBindToMonitor(SnapshotDescription snapshot,
      Configuration conf, ExceptionListener monitor) {
    long maxTime = SnapshotDescriptionUtils.getMaxMasterTimeout(conf, snapshot.getType(),
      SnapshotDescriptionUtils.DEFAULT_MAX_WAIT_TIME);
    return new OperationAttemptTimer(monitor, maxTime, snapshot);
  }

  public static void verifyAllLogsGotReferenced(FileSystem fs, Path logsDir, SnapshotDescription snapshot,
 Path snapshotLogDir) throws IOException {
    assertTrue("Logs directory doesn't exist in snapshot", fs.exists(logsDir));
    // for each of the server log dirs, make sure it matches the main directory
    Multimap<String, String> snapshotLogs = getMapOfServersAndLogs(fs, snapshotLogDir);
    Multimap<String, String> realLogs = getMapOfServersAndLogs(fs, logsDir);
    if (realLogs != null) {
      assertNotNull("No server logs added to snapshot", snapshotLogs);
    } else if (realLogs == null) {
      assertNull("Snapshotted server logs that don't exist", snapshotLogs);
    }
    // check the number of servers
    Set<Entry<String, Collection<String>>> serverEntries = realLogs.asMap().entrySet();
    Set<Entry<String, Collection<String>>> snapshotEntries = snapshotLogs.asMap().entrySet();
    assertEquals("Not the same number of snapshot and original server logs directories", serverEntries.size(), snapshotEntries.size());

    // verify we snapshotted each of the log files
    for(Entry<String, Collection<String>> serverLogs : serverEntries){
      Collection<String> snapshotServerLogs = snapshotLogs.get(serverLogs.getKey());
      assertNotNull("Snapshots missing logs for server:"+serverLogs.getKey(), snapshotServerLogs);

      // check each of the log files
      assertEquals("Didn't reference all the log files for server:" + serverLogs.getKey(),
        serverLogs.getValue().size(), snapshotServerLogs.size());
      for (String log : serverLogs.getValue()) {
        assertTrue("Snapshot logs didn't include " + log, snapshotServerLogs.contains(log));
      }
    }
  }

  private static void assertNull(String msg, Object isNull) throws CorruptedSnapshotException {
    if (isNull != null) {
      throw new CorruptedSnapshotException(msg + ", Expected " + isNull + " to be null.");
    }
  }

  private static void assertNotNull(String msg, Object notNull) throws CorruptedSnapshotException {
    if (notNull == null) {
      throw new CorruptedSnapshotException(msg + ", Expected object to not be null, but was null.");
    }
  }

  private static void assertTrue(String msg, boolean isTrue) throws CorruptedSnapshotException {
    if (!isTrue) {
      throw new CorruptedSnapshotException(msg + ", Expected true, but was false");
    }
  }

  /**
   * Assert that the expect matches the gotten amount
   * @param msg message to add the to expection
   * @param expected
   * @param gotten
   * @throws CorruptedSnapshotException thrown if the two elements don't match
   */
  private static void assertEquals(String msg, int expected, int gotten)
      throws CorruptedSnapshotException {
    if (expected != gotten) {
      throw new CorruptedSnapshotException(msg + ". Expected:" + expected + ", got:" + gotten);
    }
  }

  /**
   * @param logsDir
   */
  private static Multimap<String, String> getMapOfServersAndLogs(FileSystem fs, Path logdir)
      throws IOException {
    FileStatus[] serverLogDirs = FSUtils.listStatus(fs, logdir, new FSUtils.VisibleDirectory(fs));
    if (serverLogDirs == null) return null;
    Multimap<String, String> map = HashMultimap.create();
    for (FileStatus server : serverLogDirs) {
      FileStatus[] serverLogs = FSUtils.listStatus(fs, server.getPath(), null);
      if (serverLogs == null) continue;
      for (FileStatus log : serverLogs) {
        map.put(server.getPath().getName(), log.getPath().getName());
      }
    }
    return map;
  }
}