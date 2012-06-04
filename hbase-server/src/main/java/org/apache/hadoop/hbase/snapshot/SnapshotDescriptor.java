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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.ClusterOperation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

/**
 * SnapshotDescriptor contains the basic information for a snapshot,
 * including snapshot name, table name and the creation time.
 */
public class SnapshotDescriptor extends ClusterOperation implements Writable,
    Comparable<SnapshotDescriptor> {
  /**
   * The file contains the snapshot basic information and it
   * is under the directory of a snapshot.
   */
  public static final String SNAPSHOTINFO_FILE = ".snapshotinfo";

  private static final String SNAPSHOT_TMP_DIR = ".tmp";

  private byte[] snapshotName;
  private byte[] tableName;
  private long creationTime;

  /** Used to construct the name of the snapshot directory */
  public static final String SNAPSHOT_DIR = ".snapshot";

  /**
   * Default constructor which is only used for deserialization
   */
  public SnapshotDescriptor() {
    snapshotName = null;
    tableName = null;
    creationTime = 0L;
  }

  /**
   * Construct a SnapshotDescriptor whose creationTime is current time
   *
   * @param snapshotName identifier of snapshot
   * @param tableName table for which the snapshot is created
   */
  public SnapshotDescriptor(final byte[] snapshotName, final byte[] tableName) {
    this(snapshotName, tableName, System.currentTimeMillis());
  }

  /**
   * @param snapshotName identifier of snapshot
   * @param tableName table for which the snapshot is created
   * @param creationTime creation time of the snapshot
   */
  public SnapshotDescriptor(final byte[] snapshotName, final byte[] tableName,
      final long creationTime) {
    this.snapshotName = snapshotName;
    this.tableName = tableName;
    this.creationTime = creationTime;
  }

  /**
   * copy constrcutor
   */
  public SnapshotDescriptor(SnapshotDescriptor hsd) {
    this(hsd.snapshotName, hsd.tableName, hsd.creationTime);
  }

  /** @return name of snapshot */
  public byte[] getSnapshotName() {
    return snapshotName;
  }

  /** @return name of snapshot as String */
  public String getSnapshotNameAsString() {
    return convertNameToString(snapshotName);
  }

  /** @return name of table */
  public byte[] getTableName() {
    return tableName;
  }

  /** @return name of table as String */
  public String getTableNameAsString() {
    return Bytes.toString(tableName);
  }

  /** @return creation time of the snapshot */
  public long getCreationTime() {
    return creationTime;
  }

  /**
   * @param creationTime
   */
  public void setCreationTime(long creationTime) {
    this.creationTime = creationTime;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
   this.snapshotName = Bytes.readByteArray(in);
   this.creationTime = in.readLong();
   this.tableName = Bytes.readByteArray(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, snapshotName);
    out.writeLong(creationTime);
    Bytes.writeByteArray(out, tableName);
  }

  @Override
  public String toString() {
    return "snapshotName=" + getSnapshotNameAsString() + ", tableName=" +
      getTableNameAsString() + ", creationTime=" + getCreationTime();
  }

  /**
   * Write the snapshot descriptor information into a file under
   * <code>dir</code>
   *
   * @param snapshot snapshot descriptor
   * @param dir destination directory
   * @param fs FileSystem
   * @throws IOException
   */
  public static void write(final SnapshotDescriptor snapshot,
      final Path dir, final FileSystem fs) throws IOException {
    Path snapshotInfo = new Path(dir, SnapshotDescriptor.SNAPSHOTINFO_FILE);
    FSDataOutputStream out = fs.create(snapshotInfo, true);
    try {
      snapshot.write(out);
    } finally {
      out.close();
    }
  }

  /**
   * Get the snapshot root directory. All the snapshots are kept under this
   * directory, i.e. ${hbase.rootdir}/.snapshot
   *
   * @param rootDir hbase root directory
   * @return the base directory in which all snapshots are kept
   */
  public static Path getSnapshotRootDir(final Path rootDir) {
    return new Path(rootDir, SnapshotDescriptor.SNAPSHOT_DIR);
  }

  /**
   * Get the directory for a specified snapshot. This directory is a
   * sub-directory of snapshot root directory and all the data files for a
   * snapshot are kept under this directory.
   * @param snapshot snapshot being taken
   * @param rootDir hbase root directory
   * @return the final directory for the completed snapshot
   */
  public static Path getCompletedSnapshotDir(final SnapshotDescriptor snapshot, final Path rootDir) {
    return getCompletedSnapshotDir(snapshot.snapshotName, rootDir);
  }

  /**
   * Get the directory for a completed snapshot. This directory is a
   * sub-directory of snapshot root directory and all the data files for a
   * snapshot are kept under this directory.
   * @param snapshotName name of the snapshot being taken
   * @param rootDir hbase root directory
   * @return the final directory for the completed snapshot
   */
  public static Path getCompletedSnapshotDir(final byte[] snapshotName, final Path rootDir) {
    return getSnapshotDir(snapshotName, getSnapshotDir(rootDir));
  }

  /**
   * Get the directory to build a snapshot, before it is finalized
   * @param snapshot snapshot that will be built
   * @param rootDir root directory of the hbase installation
   * @return {@link Path} where one can build a snapshot
   */
  public static Path getWorkingSnapshotDir(SnapshotDescriptor snapshot, final Path rootDir) {
    return getSnapshotDir(snapshot.snapshotName,
      new Path(getSnapshotDir(rootDir), SNAPSHOT_TMP_DIR));
  }

  /**
   * Get the directory to store the snapshot instance
   * @param snapshotName name of the snapshot to take
   * @param snapshots hbase-global directory for storing all snapshots
   * @return
   */
  private static final Path getSnapshotDir(byte[] snapshotName, final Path snapshots) {
    return new Path(snapshots, Bytes.toString(snapshotName));
  }

  /**
   * @param rootDir hbase root directory
   * @return the directory for all completed snapshots;
   */
  public static final Path getSnapshotDir(Path rootDir) {
    return new Path(rootDir, SnapshotDescriptor.SNAPSHOT_DIR);
  }

  @Override
  public Map<String, Object> getFingerprint() {
    // no family map so fingerprinting is pointless
    return null;
  }

  @Override
  public Map<String, Object> toMap(int maxCols) {
    // not family map wrt columns
    return null;
  }

  @Override
  public int compareTo(SnapshotDescriptor other) {
    int diff = Bytes.compareTo(this.snapshotName, other.snapshotName);
    if (diff != 0) return diff;

    diff = Bytes.compareTo(tableName, other.tableName);
    if (diff != 0) return diff;

    return new Long(creationTime).compareTo(other.creationTime);
  }

  public static String convertNameToString(byte[] snapshotName) {
    return Bytes.toString(snapshotName);
  }
}
