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
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.server.commit.TwoPhaseCommit;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionCheckable;
import org.apache.hadoop.hbase.server.snapshot.error.LocalSnapshotExceptionDispatcher;
import org.apache.hadoop.hbase.server.snapshot.errorhandling.SnapshotExceptionDispatcher;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.exception.SnapshotCreationException;

/**
 * Runnable wrapper around the the snapshot operation on a <b>single</b> region so the snapshotting
 * can be done in parallel on the regions.
 * <p>
 * Handles running the snapshot on a single region and <i> just waiting for the {@link #prepare()}
 * phase to complete before considering the task complete. The remaining work({@link #commit()},
 * {@link #finish()}, etc.) still continues in another thread, but considers the task 'complete'.
 * <p>
 * To determine if all regions have completed the operation, a convenience latch is provided via
 * {@link RegionSnapshotOperationStatus#getFinishLatch()}.
 * @see RegionSnapshotOperationStatus
 */
public abstract class RegionSnapshotOperation extends
    TwoPhaseCommit<LocalSnapshotExceptionDispatcher, HBaseSnapshotException> implements
    ExceptionCheckable<HBaseSnapshotException> {
  private static final Log LOG = LogFactory.getLog(RegionSnapshotOperation.class);

  protected final SnapshotDescription snapshot;
  protected final HRegion region;

  RegionSnapshotOperation(SnapshotDescription snapshot, HRegion region,
      SnapshotExceptionDispatcher errorMonitor, long wakeFrequency) {
    super(errorMonitor, null, wakeFrequency);
    this.region = region;
    this.snapshot = snapshot;
  }

  protected SnapshotDescription getSnapshot() {
    return this.snapshot;
  }

  @Override
  public final void commit() throws HBaseSnapshotException {
    this.commitSnapshot();
    // write the committed files for the snapshot
    LOG.debug(this.region + " is committing files for snapshot.");
    try {
      this.region.addRegionToSnapshot(this.getSnapshot(), this);
    } catch (IOException e) {
      throw new SnapshotCreationException("Couldn't complete snapshot", e, this.getSnapshot());
    }
  }

  /**
   * Commit the snapshot on the region server.
   * <p>
   * Subclass hook to implement special commit logic before the region add's itself to the snapshot
   * via {@link HRegion#addRegionToSnapshot(SnapshotDescription, ErrorCheckable)}.
   */
  protected void commitSnapshot() throws HBaseSnapshotException {
    // NOOP
  }

  @Override
  public final void finish() {
    this.finishSnapshot();
  }

  /**
   * Finish the snapshot on the region server; equivalent to {@link #finish()}.
   * <p>
   * Subclass hook to implement special finish logic before the region is considered finished with a
   * snapshot.
   */
  protected void finishSnapshot() {
    // NOOP
  }

  @Override
  public void failOnError() throws HBaseSnapshotException {
    this.getErrorCheckable().failOnError();
  }

  @Override
  public boolean checkForError() {
    return this.getErrorCheckable().checkForError();
  }
}