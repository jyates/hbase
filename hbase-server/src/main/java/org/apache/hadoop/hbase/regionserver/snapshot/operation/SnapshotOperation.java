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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.server.commit.ThreePhaseCommit;
import org.apache.hadoop.hbase.server.commit.distributed.DistributedCommitException;
import org.apache.hadoop.hbase.server.commit.distributed.DistributedThreePhaseCommitErrorDispatcher;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionVisitor;
import org.apache.hadoop.hbase.server.snapshot.error.SnapshotErrorMonitorFactory;
import org.apache.hadoop.hbase.server.snapshot.errorhandling.SnapshotExceptionDispatcher;
import org.apache.hadoop.hbase.server.snapshot.task.TableInfoCopyTask;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * Base class for the the entirety of taking a single snapshot on a given regionserver
 */
public abstract class SnapshotOperation extends
ThreePhaseCommit<DistributedThreePhaseCommitErrorDispatcher, DistributedCommitException> {

/**
* {@link ExceptionVisitor} for overall operation
* {@link DistributedThreePhaseCommitErrorDispatcher}. Any failure in any of the sub-tasks
* (table-info copy, hfile referencing etc.) is propagated as a local error back to the main error
* listener, which then ensures that failure is propagated back up the distributed coordinator.
*/
private class LocalSnapshotExceptionVisitor implements
  ExceptionVisitor<DistributedThreePhaseCommitErrorDispatcher> {
@Override
public void visit(DistributedThreePhaseCommitErrorDispatcher listener, String message,
    Exception e, Object... info) {
      listener.localOperationException(SnapshotOperation.this.phase,
    wrapExceptionForSnapshot(e));
}
}

  private static final Log LOG = LogFactory.getLog(SnapshotOperation.class);
  protected final List<HRegion> regions;
  protected final SnapshotDescription snapshot;
  protected List<RegionSnapshotOperation> ops;
  protected final SnapshotTaskManager taskManager;
  protected final SnapshotExceptionDispatcher snapshotErrorListener;
  protected final FileSystem fs;
  protected final Configuration conf;

  public SnapshotOperation(DistributedThreePhaseCommitErrorDispatcher errorListener,
      long wakeFrequency, long timeout, List<HRegion> regions, SnapshotDescription snapshot,
      Configuration conf, SnapshotTaskManager taskManager,
      SnapshotErrorMonitorFactory monitorFactory, FileSystem fs) {
    super(errorListener, errorListener, wakeFrequency, timeout);
    this.snapshot = snapshot;
    this.regions = regions;
    this.taskManager = taskManager;
    this.fs = fs;
    this.conf = conf;

    // setup the error handling
    this.snapshotErrorListener = monitorFactory.createMonitorForSnapshot(snapshot);
    // and errors from local tasks are propagated to the coordinator via the generic error handler
    this.snapshotErrorListener.addErrorListener(new LocalSnapshotExceptionVisitor(),
      this.getErrorListener());
  }

  protected final void submitTableInfoCopy() throws IOException {
    taskManager.submitTask(new TableInfoCopyTask(snapshotErrorListener, snapshot, fs, FSUtils
        .getRootDir(this.conf)));
  }

  protected final DistributedCommitException wrapExceptionForSnapshot(Exception e) {
    return new DistributedCommitException(e, snapshot.toByteArray());
  }

  @Override
  public void cleanup(Exception e) {
    LOG.debug("Cleanup snapshot - handled in sub-tasks on error");
  }

  @Override
  public void finish() {
    LOG.debug("Finish snapshot - handling in subtasks on error");
  }
}
