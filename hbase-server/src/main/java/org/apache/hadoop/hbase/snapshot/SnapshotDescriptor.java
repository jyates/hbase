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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.ClusterOperation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

/**
 * SnapshotDescriptor contains the basic information for a snapshot,
 * including snapshot name, table name and the creation time.
 */
public class SnapshotDescriptor extends ClusterOperation implements Writable,
    Comparable<SnapshotDescriptor> {
  // snapshot directory constants
  /**
   * The file contains the snapshot basic information and it is under the directory of a snapshot.
   */
  public static final String SNAPSHOTINFO_FILE = ".snapshotinfo";

  private static final String SNAPSHOT_TMP_DIR = ".tmp";
  /** Used to construct the name of the snapshot directory */
  public static final String SNAPSHOT_DIR = ".snapshot";

  // snapshot operation values
  /** Default value if no start time is specified */
  public static final long NO_SNAPSHOT_START_TIME_SPECIFIED = Long.MIN_VALUE;
  
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
   * Defaults to {@value SnapshotDescriptor#DEFAULT_TIMESTAMP_SNAPSHOT_SPLIT_IN_FUTURE}
   */
  public static final String TIMESTAMP_SNAPSHOT_SPLIT_POINT_ADDITION = "hbase.snapshot.timestamp.master.splittime";
  /** Start 2 seconds in the future, if no start time given */
  public static final long DEFAULT_TIMESTAMP_SNAPSHOT_SPLIT_IN_FUTURE = 2000;

  // actual values describing a snapshot to a client
  private byte[] snapshotName;
  private byte[] tableName;
  private long creationTime;

  private Type type;

  public enum Type {
    /* globally consistent, stop-the-world snapshot */
    Global(MASTER_WAIT_TIME_GLOBAL_SNAPSHOT, REGION_WAIT_TIME_GLOBAL_SNAPSHOT),
    /* timestamp based snapshot, consistent within time sync */
    Timestamp(MASTER_WAIT_TIME_TIMESTAMP_SNAPSHOT, REGION_WAIT_TIME_TIMESTAMP_SNAPSHOT);

    private final String masterWaitConfKey;
    private final String regionWaitConfKey;

    private Type(String masterConfkey, String regionConfKey) {
      this.masterWaitConfKey = masterConfkey;
      this.regionWaitConfKey = regionConfKey;
    }

    /**
     * @param conf {@link Configuration} from which to check for the timeout
     * @param defaultMaxWaitTime Default amount of time to wait, if none is in the configuration
     * @return the max amount of time the master should wait for a snapshot to complete
     */
    public long getMaxMasterTimeout(Configuration conf, long defaultMaxWaitTime) {
      return conf.getLong(masterWaitConfKey, defaultMaxWaitTime);
    }

    /**
     * @param conf {@link Configuration} from which to check for the timeout
     * @param defaultMaxWaitTime Default amount of time to wait, if none is in the configuration
     * @return the max amount of time the region should wait for a snapshot to complete
     */
    public long getMaxRegionTimeout(Configuration conf, long defaultMaxWaitTime) {
      return conf.getLong(regionWaitConfKey, defaultMaxWaitTime);
    }
  }

  /**
   * Default constructor which is only used for deserialization
   */
  public SnapshotDescriptor() {
    snapshotName = null;
    tableName = null;
    creationTime = NO_SNAPSHOT_START_TIME_SPECIFIED;
    type = Type.Timestamp;
  }

  /**
   * Construct a SnapshotDescriptor whose creationTime is current time
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
    this(snapshotName, tableName, creationTime, Type.Timestamp);
  }

  /**
   * @param snapshotName identifier of snapshot
   * @param tableName table for which the snapshot is created
   * @param creationTime creation time of the snapshot
   * @param consistency the type of consistency to enforce on the snapshot.
   *          <p>
   *          There are currently two possible types of snapshots:
   *          <ol>
   *          <li>{@link Type#Timestamp} a timestamp-based snapshot where all key-values with
   *          timestamps up to the {@value #creationTime} are included in the snapshot (within the
   *          configured timeout). Generally, this level of consistency is all one expects from
   *          HBase - writes matching a single timestamp are all you can expect to read. <b>This is
   *          the default type of snapshot.</b>
   *          <li>{@link Type#Global} a top-the-world snapshot, blocking writes for the duration of
   *          the snapshot (and is not the advisable on a large (+1000 node) cluster due to the
   *          failure probability of a single node cause a high degree of uncertainty in successful
   *          snapshots).
   *          </ol>
   */
  public SnapshotDescriptor(final byte[] snapshotName, final byte[] tableName,
      final long creationTime, final Type consistency) {
    this.snapshotName = snapshotName;
    this.tableName = tableName;
    this.creationTime = creationTime;
    this.type = consistency;
  }

  /**
   * copy constructor
   * @param hsd snapshot description to copy
   */
  public SnapshotDescriptor(SnapshotDescriptor hsd) {
    this(hsd.snapshotName, hsd.tableName, hsd.creationTime, hsd.type);
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

    diff = new Long(creationTime).compareTo(other.creationTime);
    if (diff != 0) return diff;

    return this.type.compareTo(other.type);
  }

  public static String convertNameToString(byte[] snapshotName) {
    return Bytes.toString(snapshotName);
  }

  public void setType(Type t) {
    this.type = t;
  }

  public Type getType() {
    return this.type;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.snapshotName = Bytes.readByteArray(in);
    this.creationTime = in.readLong();
    this.tableName = Bytes.readByteArray(in);
    this.type = Type.values()[in.readInt()];
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, snapshotName);
    out.writeLong(creationTime);
    Bytes.writeByteArray(out, tableName);
    out.writeInt(this.type.ordinal());
  }

  @Override
  public String toString() {
    return "snapshotName=" + getSnapshotNameAsString() + ", tableName=" + getTableNameAsString()
        + ", creationTime=" + getCreationTime() + ", type=" + type;
  }

  /**
   * Write the snapshot descriptor information into a file under <code>dir</code>
   * @param snapshot snapshot descriptor
   * @param dir destination directory
   * @param fs FileSystem
   * @throws IOException
   */
  public static void write(final SnapshotDescriptor snapshot, final Path dir, final FileSystem fs)
      throws IOException {
    Path snapshotInfo = new Path(dir, SnapshotDescriptor.SNAPSHOTINFO_FILE);
    FSDataOutputStream out = fs.create(snapshotInfo, true);
    try {
      snapshot.write(out);
    } finally {
      out.close();
    }
  }

  /**
   * Get the snapshot root directory. All the snapshots are kept under this directory, i.e.
   * ${hbase.rootdir}/.snapshot
   * @param rootDir hbase root directory
   * @return the base directory in which all snapshots are kept
   */
  public static Path getSnapshotRootDir(final Path rootDir) {
    return new Path(rootDir, SnapshotDescriptor.SNAPSHOT_DIR);
  }

  /**
   * Get the directory for a specified snapshot. This directory is a sub-directory of snapshot root
   * directory and all the data files for a snapshot are kept under this directory.
   * @param snapshot snapshot being taken
   * @param rootDir hbase root directory
   * @return the final directory for the completed snapshot
   */
  public static Path getCompletedSnapshotDir(final SnapshotDescriptor snapshot, final Path rootDir) {
    return getCompletedSnapshotDir(snapshot.snapshotName, rootDir);
  }

  /**
   * Get the directory for a completed snapshot. This directory is a sub-directory of snapshot root
   * directory and all the data files for a snapshot are kept under this directory.
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

  public static class SnapshotBuilder {

    private final SnapshotDescriptor snapshot;
    /**
     * @param tableName
     * @param snapshotName
     */
    public SnapshotBuilder(byte[] tableName, byte[] snapshotName) {
      this.snapshot = new SnapshotDescriptor();
      this.setName(snapshotName).setTable(tableName).setType(SnapshotDescriptor.Type.Timestamp);
    }

    public SnapshotDescriptor build() {
      verify(this.snapshot);
      return this.snapshot;
    }

    /**
     * Ensure the snapshot has the minimum valid elements to be run.
     * @param snapshot descriptor to verify
     */
    private static void verify(SnapshotDescriptor snapshot) throws IllegalArgumentException { 
      checkNotNull(snapshot.tableName);
      checkNotNull(snapshot.snapshotName);
      checkNotNull(snapshot.type);
    }

    // Stealing this pattern from guava, since we don't want to have to include the jar on the
    // client just for this method
    private static void checkNotNull(Object o) throws IllegalArgumentException{
      if (o == null) throw new IllegalArgumentException("Passed reference was null!");
    }

    /**
     * Set the table that should be snapshotted
     * @param table table to snapshot
     * @return <tt>this</tt> for chaining
     */
    public SnapshotBuilder setTable(byte[] table) {
      HTableDescriptor.isLegalTableName(table);
      snapshot.tableName = table;
      return this;
    }

    /**
     * Set the table that should be snapshotted
     * @param snapshotName name of the snapshot
     * @return <tt>this</tt> for chaining
     */
    public SnapshotBuilder setName(byte[] snapshotName) {
      HTableDescriptor.isLegalTableName(snapshotName);
      snapshot.snapshotName = snapshotName;
      return this;
    }

    /**
     * Set the type of snapshot to take
     * @see Type
     * @param type type of snapshot
     * @return <tt>this</tt> for chaining
     */
    public SnapshotBuilder setType(Type type) {
      snapshot.type = type;
      return this;
    }
  }
}