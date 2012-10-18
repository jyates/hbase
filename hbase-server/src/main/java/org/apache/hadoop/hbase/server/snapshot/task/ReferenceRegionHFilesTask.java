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
package org.apache.hadoop.hbase.server.snapshot.task;

import java.io.IOException;
import java.util.TreeMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.server.snapshot.TakeSnapshotUtils;
import org.apache.hadoop.hbase.server.snapshot.error.SnapshotErrorListener;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.FSVisitor;

/**
 * Reference all the hfiles in a region for a snapshot.
 * <p>
 * Doesn't take into acccount if the hfiles are valid or not, just keeps track of what's in the
 * region's directory.
 */
public class ReferenceRegionHFilesTask extends SnapshotTask {

  public static final Log LOG = LogFactory.getLog(ReferenceRegionHFilesTask.class);
  private final Path regiondir;
  private final FileSystem fs;
  private final Path snapshotDir;

  /**
   * Reference all the files in the given region directory
   * @param snapshot snapshot for which to add references
   * @param monitor to check/send error
   * @param regionDir region directory to look for errors
   * @param fs {@link FileSystem} where the snapshot/region live
   * @param regionSnapshotDir directory in the snapshot to store region files
   */
  public ReferenceRegionHFilesTask(final SnapshotDescription snapshot,
      SnapshotErrorListener monitor, Path regionDir, final FileSystem fs, Path regionSnapshotDir) {
    super(snapshot, monitor, "Reference hfiles for region:" + regionDir.getName());
    this.regiondir = regionDir;
    this.fs = fs;
    this.snapshotDir = regionSnapshotDir;
  }

  @Override
  public void process() throws IOException {
    FSVisitor.regionStoreFiles(fs, regiondir, new FSVisitor.StoreFileVisitor() {
      private final Map<String, Path> snapshotFamilyDirs = new TreeMap<String, Path>();

      public void storeFile(final String regionInfo, final String family,
          final String hfileName) throws IOException {
        Path referenceFile = getSnapshotRefFile(family, hfileName);
        LOG.debug("Creating reference for: " + hfileName + " at " + referenceFile);
        FSUtils.touch(fs, referenceFile);
      }

      private Path getSnapshotRefFile(final String family, final String fileName) throws IOException {
        Path familyDir = this.snapshotFamilyDirs.get(family);
        if (familyDir == null) {
          familyDir = TakeSnapshotUtils.getStoreSnapshotDirectory(snapshotDir, family);
          fs.mkdirs(familyDir);
          snapshotFamilyDirs.put(family, familyDir);
        }
        return new Path(familyDir, fileName);
      }
    });

    if (LOG.isDebugEnabled()) {
      LOG.debug("Finished referencing hfiles, current fs state:");
      FSUtils.logFileSystemState(fs, fs.getWorkingDirectory(), LOG);
    }
  }
}