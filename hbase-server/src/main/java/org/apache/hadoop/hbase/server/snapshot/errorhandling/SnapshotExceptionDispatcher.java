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
package org.apache.hadoop.hbase.server.snapshot.errorhandling;

import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.server.errorhandling.*;
import org.apache.hadoop.hbase.server.errorhandling.impl.ExceptionDispatcher;
import org.apache.hadoop.hbase.server.errorhandling.impl.delegate.DelegatingExceptionDispatcher;
import org.apache.hadoop.hbase.server.snapshot.error.SnapshotErrorListener;
import org.apache.hadoop.hbase.server.snapshot.error.SnapshotFailureListener;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.exception.SnapshotCreationException;

/**
 * Wrapper class to pass snapshot errors onto a general listener.
 */
public class SnapshotExceptionDispatcher implements SnapshotFailureListener {
  /**
   * {@link SnapshotErrorListener} that just delegates all error handling to an an underlying
   * dispatcher
   */
  public static class DelegatingDispatcher
      extends
      DelegatingExceptionDispatcher<ExceptionDispatcher<SnapshotFailureListener, HBaseSnapshotException>, SnapshotFailureListener, HBaseSnapshotException>
      implements SnapshotErrorListener {

    private final SnapshotExceptionDispatcher handler;

    /**
     * Create an error monitor for a snapshot that inherently will dispatch errors to any currently
     * bound listeners
     * @param delegate to update with snapshot errors
     */
    public DelegatingDispatcher(
        ExceptionDispatcher<SnapshotFailureListener, HBaseSnapshotException> delegate) {
      super(delegate);
      this.handler = new SnapshotExceptionDispatcher(this);
    }

    @Override
    public void snapshotFailure(String reason, SnapshotDescription snapshot) {
      handler.snapshotFailure(reason, snapshot);
    }

    @Override
    public void snapshotFailure(String reason, SnapshotDescription snapshot, Exception t) {
      handler.snapshotFailure(reason, snapshot, t);
    }

    @Override
    public void snapshotFailure(String reason, String snapshotName) {
      handler.snapshotFailure(reason, snapshotName);
    }
  }

  /**
   * Generic {@link SnapshotErrorListener} that passes specific snapshot errors to a generic
   * listener
   */
  public static class GenericDispatcher extends
      ExceptionDispatcher<SnapshotFailureListener, HBaseSnapshotException> implements
      SnapshotFailureListener {

    private final SnapshotExceptionDispatcher handler;

    /**
     * Create an error monitor for a snapshot that inherently will dispatch errors to any currently
     * bound listeners
     * @param visitor to apply to {@link SnapshotErrorListener} when propaagting failures
     */
    public GenericDispatcher(ExceptionVisitor<SnapshotFailureListener> visitor) {
      super(visitor);
      this.handler = new SnapshotExceptionDispatcher(this);
    }

    @Override
    public void snapshotFailure(String reason, SnapshotDescription snapshot) {
      handler.snapshotFailure(reason, snapshot);
    }

    @Override
    public void snapshotFailure(String reason, SnapshotDescription snapshot, Exception t) {
      handler.snapshotFailure(reason, snapshot, t);
    }

    @Override
    public void snapshotFailure(String reason, String snapshotName) {
      handler.snapshotFailure(reason, snapshotName);
    }
  }

  private ExceptionListener<HBaseSnapshotException> listener;

  private SnapshotExceptionDispatcher(ExceptionListener<HBaseSnapshotException> listener) {
    this.listener = listener;
  }

  @Override
  public void snapshotFailure(String reason, SnapshotDescription snapshot) {
    this.snapshotFailure(reason, snapshot, new SnapshotCreationException(reason, snapshot));
  }

  @Override
  public void snapshotFailure(String reason, SnapshotDescription snapshot, Exception t) {
    listener.receiveError(reason,
      SnapshotExceptionVisitor.getExceptionAsSnapshotException(reason, snapshot, t), snapshot);
  }

  @Override
  public void snapshotFailure(String reason, String snapshotName) {
    listener.receiveError(reason, new HBaseSnapshotException("Failed snapshot:" + snapshotName
        + " because " + reason), snapshotName);
  }
}
