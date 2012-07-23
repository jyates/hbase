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
package org.apache.hadoop.hbase.regionserver.snapshot.operation;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;
import org.apache.hadoop.hbase.snapshot.monitor.SnapshotFailureListener;

/**
 * Snapshot a region in such a way that ensures global consistency among all regions serving the
 * table.
 */
public class GlobalRegionSnapshotOperation extends RegionSnapshotOperation {
  private static final Log LOG = LogFactory.getLog(GlobalRegionSnapshotOperation.class);

  protected final GlobalRegionSnapshotProgressMonitor status;

  public GlobalRegionSnapshotOperation(SnapshotDescriptor snapshot, HRegion region,
      SnapshotFailureListener errorMonitor, GlobalRegionSnapshotProgressMonitor status,
      long wakeFrequency) {
    super(snapshot, region, errorMonitor, wakeFrequency, status);
    this.status = status;
  }

  @Override
  public void prepare() throws SnapshotCreationException {
    try {
      region.startGloballyConsistentSnapshot(snapshot, status, this);
    } catch (IOException e) {
      throw new SnapshotCreationException(e);
    }
  }

  @Override
  public void commit() {
    // noop
  }

  @Override
  public void cleanup() {
    LOG.debug("Cleaning up snapshot for region");
    region.finishGlobalSnapshot();
  }

  public void commitSnapshot() {
    this.getCommitLatch().countDown();
  }
}
