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
package org.apache.hadoop.hbase.server.commit.distributed;

import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.DaemonThreadFactory;
import org.apache.hadoop.hbase.protobuf.generated.DistributedCommitProtos.CommitPhase;
import org.apache.hadoop.hbase.protobuf.generated.ErrorHandlingProtos.RemoteFailureException;
import org.apache.hadoop.hbase.server.commit.ThreePhaseCommit;
import org.apache.hadoop.hbase.server.commit.distributed.controller.DistributedCommitController;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Run a Distributed {@link ThreePhaseCommit} on demand from a controller
 * @param <C> controller to use for notifying distributed members of the operation
 * @param <T> Type of task that is being submitted
 * @param <L> Type of listener to keep track of the operation progress
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DistributedThreePhaseCommitManager<C extends DistributedCommitController<?>, T extends Callable<?>, L extends DistributedThreePhaseCommitErrorListener>
    implements Closeable {
  private static final Log LOG = LogFactory.getLog(DistributedThreePhaseCommitManager.class);
  private final Map<String, RunningOperation> operations = new HashMap<String, RunningOperation>();
  private final ExecutorService pool;
  protected final C controller;
  protected final RemoteExceptionSerializer serializer;

  /**
   * Create a manager with the specified controller, name and pool.
   * <p>
   * Exposed for testing!
   * @param controller control access to the rest of the cluster
   * @param nodeName name of the node
   * @param pool thread pool to which new tasks are submitted
   */
  public DistributedThreePhaseCommitManager(C controller, String nodeName, ThreadPoolExecutor pool) {
    this.controller = controller;
    this.serializer = new RemoteExceptionSerializer(nodeName);
    this.pool = pool;
  }

  /**
   * Create a manager with the speciied controller, node name and parameters for a standard thread
   * pool
   * @param controller control access to the rest of the cluster
   * @param nodeName name of the node where <tt>this</tt> is running
   * @param keepAliveTime amount of time (ms) idle threads should be kept alive
   * @param opThreads number of threads that should be used for running tasks
   * @param poolName prefix name that should be used for the pool
   */
  public DistributedThreePhaseCommitManager(C controller, String nodeName, long keepAliveTime,
      int opThreads, String poolName) {
    this(controller, nodeName, new ThreadPoolExecutor(0, opThreads, keepAliveTime,
        TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(), new DaemonThreadFactory("(" + poolName
            + ")-3PC commit-pool")));
  }

  @Override
  public void close() {
    // have to use shutdown now to break any latch waiting
    pool.shutdownNow();
  }

  /**
   * Submit an operation and all its dependent operations to be run.
   * @param errorMonitor monitor to notify if we can't start all the operations
   * @param primary primary operation to start
   * @param tasks subtasks of the primary operation
   * @return <tt>true</tt> if the operation was started correctly, <tt>false</tt> if the primary
   *         task or any of the sub-tasks could not be started. In the latter case, if the pool is
   *         full the error monitor is also notified that the task could not be created via a
   *         {@link DistributedThreePhaseCommitErrorListener#localOperationException(CommitPhase, Exception)}
   */
  protected boolean submitOperation(L errorMonitor, String operationName, T primary,
      Callable<?>... tasks) {
    // if the submitted task was null, then we don't want to run the subtasks
    if (primary == null) return false;

    // make sure we aren't already running an operation of that name
    synchronized (operations) {
      if (operations.get(operationName) != null) return false;
    }

    // kick off all the tasks
    List<Future<?>> futures = new ArrayList<Future<?>>((tasks == null ? 0 : tasks.length) + 1);
    try {
      futures.add(this.pool.submit((Callable<?>) primary));
      for (Callable<?> task : tasks) {
        futures.add(this.pool.submit(task));
      }
      // if everything got started properly, we can add it known running operations
      synchronized (operations) {
        this.operations.put(operationName, new RunningOperation(primary, errorMonitor));
      }
      return true;
    } catch (RejectedExecutionException e) {
      // the thread pool is full and we can't run the operation
      errorMonitor.localOperationException(CommitPhase.PRE_PREPARE, new DistributedCommitException(
          "Operation pool is full!", e));
      // cancel all operations proactively
      for (Future<?> future : futures) {
        future.cancel(true);
      }
    }
    return false;
  }

  /**
   * The connection to the rest of the commit group (cohort and coordinator) has been
   * broken/lost/failed. This should fail any interested operations, but not attempt to notify other
   * members since we cannot reach them anymore.
   * @param message description of the error
   * @param cause the actual cause of the failure
   */
  public void controllerConnectionFailure(final String message, final IOException cause) {
    new NotifyAllListeners() {
      public void notifyListener(L listener) {
        listener.controllerConnectionFailure(message, cause);
      }
    }.run();
  }

  /**
   * Abort the operation with the given name
   * @param opName name of the operation to about
   * @param data serialized information about the abort
   */
  public void abortOperation(String opName, byte[] data) {
    // figure out the data we need to pass
    final RemoteFailureException[] remote = new RemoteFailureException[1];
    try {
      remote[0] = RemoteFailureException.parseFrom(data);
    } catch (InvalidProtocolBufferException e) {
      LOG.warn("Got an error notification for op:" + opName
          + " but we can't read the information. Killing the operation.");
      // we got a remote exception, but we can't describe it, so build a generic one
      remote[0] = serializer.buildFromUnreadableRemoteException(e);
    }
    // if we know about the operation, notify it
    new NotifyListener(opName) {
      @Override
      protected void notifyListener(L listener) {
        listener.remoteCommitError(remote[0]);
      }
    }.run();
  }

  private class RunningOperation {
    private final WeakReference<L> errorListener;
    private final WeakReference<T> op;

    public RunningOperation(T op, L listener) {
      this.errorListener = new WeakReference<L>(listener);
      this.op = new WeakReference<T>(op);
    }

    public boolean isRemovable() {
      return errorListener.get() == null && this.op.get() == null;
    }
  }

  /**
   * Simple helper class to thread-safely notify listeners of a change in the operation state
   */
  private abstract class NotifyRunner implements Runnable {

    /**
     * Log a warning if the operation or the listener is null but the other isn't null.
     * @param op operation to check
     * @param listener listener to check
     */
    protected void logMismatchedNulls(T op, L listener) {
      if (op == null && listener != null || op != null && listener == null) {
        LOG.warn("Operation is currently null:" + (op == null) + ", but listener is "
            + (listener == null ? "" : "not") + "null -- Possible memory leak.");
      }
    }

    /**
     * Notify the running operation of some state change. Subclass hook for adding custom
     * notifications.
     * @param operation a non-null, running operation to notify.
     */
    protected void notifyOperation(T operation) {
    }

    /**
     * Notify the listener of some state change. Subclass hook for adding custom notifications.
     * @param listener errorListener of a non-null, running operation to notify.
     */
    protected void notifyListener(L errorListener) {
    }
  }

  /**
   * Apply {@link #notify()} to all listeners, synchronizing on the passed {@link Map}.
   */
  protected abstract class NotifyAllListeners extends NotifyRunner {

    @Override
    public void run() {
      List<String> remove = new ArrayList<String>();
      Map<String, RunningOperation> toNotify = new HashMap<String, RunningOperation>();
      synchronized (operations) {
        for (Entry<String, RunningOperation> op : operations.entrySet()) {
          if (op.getValue().isRemovable()) {
            LOG.warn("Ignoring error notification for operation:" + op.getKey()
                + "  because we the op has finished already.");
            remove.add(op.getKey());
            continue;
          }
          toNotify.put(op.getKey(), op.getValue());
        }
      }
      for (Entry<String, RunningOperation> running : toNotify.entrySet()) {
        T op = running.getValue().op.get();
        L listener = running.getValue().errorListener.get();
        logMismatchedNulls(op, listener);
        if (op == null) {
          // if the op is null, we probably don't have any more references, so we should check again
          if (running.getValue().isRemovable()) {
            remove.add(running.getKey());
            continue;
          }

        }
        // notify the elements, if they aren't null
        if (op != null) notifyOperation(op);
        if (listener != null) notifyListener(listener);
      }

      // clear out operations that have finished
      synchronized (operations) {
        for (String op : remove) {
          operations.remove(op);
        }
      }
    }
  }

  /**
   * Helper class to notify a single operation listeners of a change. Synchronized on the passed
   * {@link Map}.
   */
  protected abstract class NotifyListener extends NotifyRunner {

    private final String prefix;

    /**
     * Create a notify operator to notify any listeners listening for the given operation.
     * @param operationName name of the operation to notify
     */
    public NotifyListener(String operationName) {
      this.prefix = operationName;
    }

    @Override
    public void run() {
      RunningOperation running;
      T op;
      L listener;
      synchronized (operations) {
        running = operations.get(prefix);
        if (running == null) {
          LOG.warn("Don't know about op:" + prefix + ", ignoring notification attempt.");
          return;
        }
        if (running.isRemovable()) {
          operations.remove(prefix);
          return;
        }
      }
      op = running.op.get();
      listener = running.errorListener.get();
      logMismatchedNulls(op, listener);
      if (op != null) {
        notifyOperation(op);
      }
      if (listener != null) {
        notifyListener(listener);
      }
    }
  }

  /**
   * Exposed for testing!
   * @return get the underlying thread pool.
   */
  public ExecutorService getThreadPool() {
    return this.pool;
  }
}