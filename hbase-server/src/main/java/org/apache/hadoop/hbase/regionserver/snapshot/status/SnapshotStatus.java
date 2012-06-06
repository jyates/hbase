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
package org.apache.hadoop.hbase.regionserver.snapshot.status;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.regionserver.snapshot.monitor.DelegatingSnapshotErrorMonitor;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;

/**
 * Standard interface for snapshot status tracking
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class SnapshotStatus extends DelegatingSnapshotErrorMonitor {

  private Future<?> future;

  /**
   * Generally used for logging
   * @return String representation of the current progress.
   */
  public abstract String getStatus();

  /**
   * Check to see if the snapshot has completed on all regions
   * @return <tt>true</tt> if it has
   * @throws SnapshotCreationException if the snapshot has failed at any point
   */
  public final boolean isDone() throws SnapshotCreationException {
    // XXX this is really ugly - needs to be refactored for a status that
    // monitors the progress of a thread and one that monitors check done
    if (checkDone()) {
      if (future == null) return true;
      return future.isDone();
    }
    return false;
  }

  /**
   * Internal method to check the status of the operation.
   * <p>
   * Subclasses should implement this method for custom done behavior
   * @return by default, <tt>true</tt>
   * @throws SnapshotCreationException
   */
  protected boolean checkDone() throws SnapshotCreationException {
    return true;
  }

  /**
   * Set the future this operation should poll to see if its finished
   * <p>
   * Must be called before checking {@link #isDone()}
   * @param result future for the operation in the pool. This is polled in
   *          {@link #isDone()} to check status
   */
  public void setFuture(Future<?> result) {
    this.future = result;
  }

  /**
   * Waits if necessary for the computation to complete, and then retrieves its
   * result.
   * @return the result of the computation set on for this status to monitor
   * @throws CancellationException if the computation was cancelled
   * @throws ExecutionException if the computation threw an exception
   * @throws InterruptedException if the current thread was interrupted while
   *           waiting
   */
  public Object getResult() throws CancellationException, InterruptedException, ExecutionException {
    return this.future.get();
  }

  /**
   * Cancel the underlying operation that this status is monitoring.
   * <p>
   * Interrupts the computation, preventing it from cleanly completing.
   * @return <tt>false</tt> if the task could not be cancelled, typically
   *         because it has already completed normally; <tt>true</tt> otherwise
   */
  public boolean cancel() {
    return this.future.cancel(true);
  }

}
