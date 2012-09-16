/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.server.snapshot.error;

import java.util.List;

import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionVisitor;
import org.apache.hadoop.hbase.server.errorhandling.FaultInjector;
import org.apache.hadoop.hbase.server.errorhandling.impl.ExceptionDispatcher;
import org.apache.hadoop.hbase.server.errorhandling.impl.ExceptionOrchestrator;
import org.apache.hadoop.hbase.server.errorhandling.impl.ExceptionOrchestratorFactory;
import org.apache.hadoop.hbase.server.errorhandling.impl.InjectingExceptionDispatcher;
import org.apache.hadoop.hbase.server.snapshot.errorhandling.SnapshotExceptionDispatcher;
import org.apache.hadoop.hbase.server.snapshot.errorhandling.SnapshotExceptionDispatcher.DelegatingDispatcher;
import org.apache.hadoop.hbase.server.snapshot.errorhandling.SnapshotExceptionVisitor;
import org.apache.hadoop.hbase.server.snapshot.errorhandling.SnapshotExceptionVisitor.BoundSnapshotErrorVisitor;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;

/**
 * Factory to create a monitor for a running snapshot
 */
public class SnapshotErrorMonitorFactory
    extends
    ExceptionOrchestratorFactory<ExceptionDispatcher<SnapshotFailureListener, HBaseSnapshotException>, SnapshotFailureListener> {
  private ExceptionOrchestrator<HBaseSnapshotException> hub;

  public SnapshotErrorMonitorFactory() {
    super(new SnapshotExceptionVisitor());
    this.hub = new ExceptionOrchestrator<HBaseSnapshotException>("running snapshot error hub");
  }

  /**
   * Create an snapshot error monitor for the given snapshot. Errors bound for other snapshots are
   * ignored, but generic errors (without a snapshot in the error information) are considered valid
   * and accepted.
   * <p>
   * This allows the parent to propagate an error (for instance, an abort notification) down to all
   * running snapshots.
   * @param snapshot snapshot to bind to the monitor
   * @return an error monitor for the given snapshot.
   */
  public synchronized SnapshotErrorListener createMonitorForSnapshot(SnapshotDescription snapshot) {
    return createSnapshotDispatcher(new BoundSnapshotErrorVisitor(snapshot));
  }

  /**
   * Create a generic error monitor that is still bound to the main error dispatcher. Other than
   * accepting errors for all snapshots (rather than a single snapshot), it acts in all respects
   * like the {@link SnapshotErrorListener} from
   * {@link #createMonitorForSnapshot(SnapshotDescription)}.
   * @return a snapshot error monitor that accepts errors for all snapshots
   */
  public synchronized SnapshotErrorListener createGenericSnapshotErrorMonitor() {
    return createSnapshotDispatcher(new SnapshotExceptionVisitor());
  }

  private SnapshotErrorListener createSnapshotDispatcher(SnapshotExceptionVisitor visitor) {
    ExceptionDispatcher<SnapshotFailureListener, HBaseSnapshotException> handler = createErrorHandler();
    DelegatingDispatcher listener = new SnapshotExceptionDispatcher.DelegatingDispatcher(handler);
    // propagate errors from the hub to the listener
    this.getHub().addErrorListener(visitor, listener);
    // propagate errors from the listener to the hub
    listener.addErrorListener(handler.genericVisitor, this.getHub());
    return listener;
  }

  public ExceptionOrchestrator<HBaseSnapshotException> getHub() {
    return this.hub;
  }

  @Override
  protected ExceptionDispatcher<SnapshotFailureListener, HBaseSnapshotException> buildErrorHandler(
      ExceptionVisitor<SnapshotFailureListener> visitor) {
    return new SnapshotExceptionDispatcher.GenericDispatcher(visitor);
  }

  @Override
  protected ExceptionDispatcher<SnapshotFailureListener, HBaseSnapshotException> wrapWithInjector(
      ExceptionDispatcher<SnapshotFailureListener, HBaseSnapshotException> delegate,
      List<FaultInjector<?>> injectors) {
    return new InjectingExceptionDispatcher<ExceptionDispatcher<SnapshotFailureListener, HBaseSnapshotException>, SnapshotFailureListener, HBaseSnapshotException>(
        delegate, injectors);
  }
}