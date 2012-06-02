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
package org.apache.hadoop.hbase.regionserver.snapshot;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.snapshot.status.SnapshotFailureMonitor;
import org.apache.hadoop.hbase.regionserver.snapshot.status.TableInfoStatus;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;
import org.apache.hadoop.hbase.util.FSTableDescriptors;

/**
 * Copy the table info into the snapshot directory
 */
public class TableInfoCopyOperation extends SnapshotOperation<TableInfoStatus> {

  public static final Log LOG = LogFactory.getLog(TableInfoCopyOperation.class);
  private final RegionServerServices rss;
  private final FileSystem fs;

  /**
   * @param failures
   * @param snapshot
   * @param rss
   */
  public TableInfoCopyOperation(SnapshotFailureMonitor failures, SnapshotDescriptor snapshot,
      RegionServerServices rss) {
    super(failures, snapshot);
    this.rss = rss;
    this.fs = rss.getFileSystem();
    this.setStatus(new TableInfoStatus());
  }

  @Override
  public void run() {
    try {
      // 0. get the HTable descriptor
      HTableDescriptor orig = FSTableDescriptors.getTableDescriptor(fs, rss.getRootDir(),
        this.snapshot.getTableName());
      checkFailure();
      // 1. write a copy of it to the snapshot directory
      Path snapshotDir = SnapshotDescriptor.getWorkingSnapshotDir(snapshot, rss.getRootDir());
      FSTableDescriptors.createTableDescriptor(fs, snapshotDir, orig, true);
    } catch (IOException e) {
      failSnapshot("Couldn't copy tableinfo", e);
    } catch (NullPointerException e) {
      failSnapshot("Couldn't copy tableinfo", e);
    }
    LOG.debug("Finished copying tableinfo.");
  }
}
