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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.snapshot.exception.CorruptedSnapshotException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * Utility class to help manage {@link SnapshotDescription SnapshotDesriptions}.
 */
public class SnapshotDescriptionUtils {

  private static final Log LOG = LogFactory.getLog(SnapshotDescriptionUtils.class);

  // snapshot directory constants
  /**
   * The file contains the snapshot basic information and it is under the directory of a snapshot.
   */
  public static final String SNAPSHOTINFO_FILE = ".snapshotinfo";

  private static final String SNAPSHOT_TMP_DIR = ".tmp";
  // snapshot operation values
  /** Default value if no start time is specified */
  public static final long NO_SNAPSHOT_START_TIME_SPECIFIED = 0;

  public static final String MASTER_WAIT_TIME_GLOBAL_SNAPSHOT = "hbase.snapshot.global.master.timeout";
  public static final String REGION_WAIT_TIME_GLOBAL_SNAPSHOT = "hbase.snapshot.global.region.timeout";
  public static final String MASTER_WAIT_TIME_TIMESTAMP_SNAPSHOT = "hbase.snapshot.timestamp.master.timeout";
  public static final String REGION_WAIT_TIME_TIMESTAMP_SNAPSHOT = "hbase.snapshot.timestamp.region.timeout";

  /** Default timeout of 60 sec for a snapshot timeout on a region */
  public static final long DEFAULT_REGION_SNAPSHOT_TIMEOUT = 60000;

  /** By default, wait 60 seconds for a snapshot to complete */
  public static final long DEFAULT_MAX_WAIT_TIME = 60000;

  /**
   * Conf key for amount of time the in the future a timestamp snapshot should be taken (ms).
   * Defaults to {@value SnapshotDescriptionUtils#DEFAULT_TIMESTAMP_SNAPSHOT_SPLIT_IN_FUTURE}
   */
  public static final String TIMESTAMP_SNAPSHOT_SPLIT_POINT_ADDITION = "hbase.snapshot.timestamp.master.splittime";
  /** Start 2 seconds in the future, if no start time given */
  public static final long DEFAULT_TIMESTAMP_SNAPSHOT_SPLIT_IN_FUTURE = 2000;

  private SnapshotDescriptionUtils() {
    // private constructor for utility class
  }

  /**
   * Check to make sure that the description of the snapshot requested is valid
   * @param snapshot description of the snapshot
   * @throws IllegalArgumentException if the name of the snapshot or the name of the table to
   *           snapshot are not valid names.
   */
  public static void assertSnapshotRequestIsValid(SnapshotDescription snapshot)
      throws IllegalArgumentException {
    // FIXME these method names is really bad - trunk will probably change
    // make sure the snapshot name is valid
    HTableDescriptor.isLegalTableName(Bytes.toBytes(snapshot.getName()));
    // make sure the table name is valid
    HTableDescriptor.isLegalTableName(Bytes.toBytes(snapshot.getTable()));
  }

  /**
   * @param conf {@link Configuration} from which to check for the timeout
   * @param type type of snapshot being taken
   * @param defaultMaxWaitTime Default amount of time to wait, if none is in the configuration
   * @return the max amount of time the master should wait for a snapshot to complete
   */
  public static long getMaxMasterTimeout(Configuration conf, SnapshotDescription.Type type,
      long defaultMaxWaitTime) {
    String confKey;
    switch (type) {
    case GLOBAL:
      confKey = MASTER_WAIT_TIME_GLOBAL_SNAPSHOT;
      break;
    case TIMESTAMP:
    default:
      confKey = MASTER_WAIT_TIME_TIMESTAMP_SNAPSHOT;
    }
    return conf.getLong(confKey, defaultMaxWaitTime);
  }

  /**
   * @param conf {@link Configuration} from which to check for the timeout
   * @param type type of snapshot being taken
   * @param defaultMaxWaitTime Default amount of time to wait, if none is in the configuration
   * @return the max amount of time the region should wait for a snapshot to complete
   */
  public static long getMaxRegionTimeout(Configuration conf, SnapshotDescription.Type type,
      long defaultMaxWaitTime) {
    String confKey;
    switch (type) {
    case GLOBAL:
      confKey = REGION_WAIT_TIME_GLOBAL_SNAPSHOT;
      break;
    case TIMESTAMP:
    default:
      confKey = REGION_WAIT_TIME_TIMESTAMP_SNAPSHOT;
    }
    return conf.getLong(confKey, defaultMaxWaitTime);
  }

  /**
   * Get the snapshot root directory. All the snapshots are kept under this directory, i.e.
   * ${hbase.rootdir}/.snapshot
   * @param rootDir hbase root directory
   * @return the base directory in which all snapshots are kept
   */
  public static Path getSnapshotRootDir(final Path rootDir) {
    return new Path(rootDir, HConstants.SNAPSHOT_DIR);
  }

  /**
   * Get the directory for a specified snapshot. This directory is a sub-directory of snapshot root
   * directory and all the data files for a snapshot are kept under this directory.
   * @param snapshot snapshot being taken
   * @param rootDir hbase root directory
   * @return the final directory for the completed snapshot
   */
  public static Path getCompletedSnapshotDir(final SnapshotDescription snapshot, final Path rootDir) {
    return getCompletedSnapshotDir(snapshot.getName(), rootDir);
  }

  /**
   * Get the directory for a completed snapshot. This directory is a sub-directory of snapshot root
   * directory and all the data files for a snapshot are kept under this directory.
   * @param snapshotName name of the snapshot being taken
   * @param rootDir hbase root directory
   * @return the final directory for the completed snapshot
   */
  public static Path getCompletedSnapshotDir(final byte[] snapshotName, final Path rootDir) {
    return getSnapshotDir(getSnapshotDir(rootDir), Bytes.toString(snapshotName));
  }

  /**
   * Get the directory for a completed snapshot. This directory is a sub-directory of snapshot root
   * directory and all the data files for a snapshot are kept under this directory.
   * @param snapshotName name of the snapshot being taken
   * @param rootDir hbase root directory
   * @return the final directory for the completed snapshot
   */
  public static Path getCompletedSnapshotDir(final String snapshotName, final Path rootDir) {
    return getSnapshotDir(getSnapshotDir(rootDir), snapshotName);
  }

  /**
   * Get the directory to build a snapshot, before it is finalized
   * @param snapshot snapshot that will be built
   * @param rootDir root directory of the hbase installation
   * @return {@link Path} where one can build a snapshot
   */
  public static Path getWorkingSnapshotDir(SnapshotDescription snapshot, final Path rootDir) {
    return getSnapshotDir(new Path(getSnapshotDir(rootDir), SNAPSHOT_TMP_DIR), snapshot.getName());
  }

  /**
   * Get the directory to store the snapshot instance
   * @param snapshotsDir hbase-global directory for storing all snapshots
   * @param snapshotName name of the snapshot to take
   * @return
   */
  private static final Path getSnapshotDir(final Path snapshotsDir, String snapshotName) {
    return new Path(snapshotsDir, snapshotName);
  }

  /**
   * @param rootDir hbase root directory
   * @return the directory for all completed snapshots;
   */
  public static final Path getSnapshotDir(Path rootDir) {
    return new Path(rootDir, HConstants.SNAPSHOT_DIR);
  }

  /**
   * Create a proper snapshot description based on the based general descriptor for a snapshot
   * @param snapshot general snapshot descriptor
   * @param conf Configuration to read configured snapshot defaults if snapshot is not complete
   * @return a valid snapshot description
   * @throws IllegalArgumentException if the {@link SnapshotDescription} is not a complete
   *           {@link SnapshotDescription}.
   */
  public static SnapshotDescription validate(SnapshotDescription snapshot, Configuration conf)
      throws IllegalArgumentException {
    if (!snapshot.hasTable()) throw new IllegalArgumentException(
        "Descriptor doesn't apply to a table, so we can't build it.");

    // set the creation time, if one hasn't been set
    long time = snapshot.getCreationTime();
    if (time == SnapshotDescriptionUtils.NO_SNAPSHOT_START_TIME_SPECIFIED) {
      time = EnvironmentEdgeManager.currentTimeMillis();
      if (snapshot.getType().equals(SnapshotDescription.Type.TIMESTAMP)) {
        long increment = conf.getLong(
          SnapshotDescriptionUtils.TIMESTAMP_SNAPSHOT_SPLIT_POINT_ADDITION,
          SnapshotDescriptionUtils.DEFAULT_TIMESTAMP_SNAPSHOT_SPLIT_IN_FUTURE);
        LOG.debug("Setting timestamp snasphot in future by " + increment + " ms.");
        time += increment;
      }
      LOG.debug("Creation time not specified, setting to:" + time + " (current time:"
          + EnvironmentEdgeManager.currentTimeMillis() + ").");
      SnapshotDescription.Builder builder = snapshot.toBuilder();
      builder.setCreationTime(time);
      snapshot = builder.build();
    }
    return snapshot;
  }

  /**
   * Write the snapshot description into the working directory of a snapshot
   * @param snapshot description of the snapshot being taken
   * @param workingDir working directory of the snapshot
   * @param fs {@link FileSystem} on which the snapshot should be taken
   * @throws IOException if we can't reach the filesystem and the file cannot be cleaned up on
   *           failure
   */
  public static void writeSnasphotInfo(SnapshotDescription snapshot, Path workingDir, FileSystem fs)
      throws IOException {
    Path snapshotInfo = new Path(workingDir, SnapshotDescriptionUtils.SNAPSHOTINFO_FILE);
    try {
      FSDataOutputStream out = fs.create(snapshotInfo, true);
      try {
        snapshot.writeTo(out);
      } finally {
        out.close();
      }
    } catch (IOException e) {
      // if we get an exception, try to remove the snapshot info
      if (!fs.delete(snapshotInfo, false)) {
        // if we didn't delete the file, then throw an error
        if (fs.exists(snapshotInfo)) {
          throw new IOException("Couldn't delete snapshot info file: " + snapshotInfo);
        }
      }
    }
  }

  /**
   * Read in the {@link SnapshotDescription} stored for the snapshot in the passed directory
   * @param fs filesystem where the snapshot was taken
   * @param snapshotDir directory where the snapshot was stored
   * @return the stored snapshot description
   * @throws CorruptedSnapshotException if the snapshot cannot be read
   */
  public static SnapshotDescription readSnapshotInfo(FileSystem fs, Path snapshotDir)
      throws CorruptedSnapshotException {
    Path snapshotInfo = new Path(snapshotDir, SNAPSHOTINFO_FILE);
    try {
      FSDataInputStream in = null;
      try {
        in = fs.open(snapshotInfo);
        return SnapshotDescription.parseFrom(in);
      } finally {
        if (in != null) in.close();
      }
    } catch (IOException e) {
      throw new CorruptedSnapshotException("Couldn't read snapshot info from:" + snapshotInfo, e);
    }
  }
}