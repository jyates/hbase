/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver.snapshot.operation;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;
import org.apache.hadoop.hbase.snapshot.monitor.SnapshotFailureListener;
import org.apache.hadoop.hbase.util.FSTableDescriptors;

/**
 * Copy the table info into the snapshot directory
 */
public class TableInfoCopyOperation extends SnapshotOperation {

  public static final Log LOG = LogFactory.getLog(TableInfoCopyOperation.class);
  private final FileSystem fs;
  private final Path rootDir;

  /**
   * Copy the table info for the given table into the snapshot
   * @param failureListener listen for errors while running the snapshot
   * @param snapshot snapshot for which we are copying the table info
   * @param fs {@link FileSystem} where the tableinfo is stored (and where the copy will be written)
   * @param rootDir root of the {@link FileSystem} where the tableinfo is stored
   */
  public TableInfoCopyOperation(SnapshotFailureListener failureListener,
      SnapshotDescriptor snapshot,
      FileSystem fs, Path rootDir) {
    super(snapshot, failureListener);
    this.rootDir = rootDir;
    this.fs = fs;
  }

  @Override
  public void run() {
    try {
      LOG.debug("Attempting to copy table info for snapshot:" + this.snapshot);
      // 0. get the HTable descriptor
      HTableDescriptor orig = FSTableDescriptors.getTableDescriptor(fs, rootDir,
        this.snapshot.getTableName());
      if (this.checkForError()) {
        LOG.error("Found an external error, quiting copying the table info.");
        return;
      }
      // 1. write a copy of it to the snapshot directory
      Path snapshotDir = SnapshotDescriptor.getWorkingSnapshotDir(snapshot, rootDir);
      FSTableDescriptors.createTableDescriptor(fs, snapshotDir, orig, false);
    } catch (IOException e) {
      snapshotFailure("Couldn't copy tableinfo", e);
    } catch (NullPointerException e) {
      snapshotFailure("Couldn't copy tableinfo", e);
    }
    LOG.debug("Finished copying tableinfo.");
  }
}
