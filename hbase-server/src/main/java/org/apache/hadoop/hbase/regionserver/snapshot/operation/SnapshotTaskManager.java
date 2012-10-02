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

import java.io.Closeable;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.DaemonThreadFactory;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.regionserver.snapshot.RegionServerSnapshotHandler;
import org.apache.hadoop.hbase.server.Aborting;

/**
 * Handle running each of the individual tasks for completing a snapshot on a regionserver.
 */
public class SnapshotTaskManager implements Closeable, Abortable {
  private static final Log LOG = LogFactory.getLog(SnapshotTaskManager.class);

  /** Maximum number of concurrent snapshot region tasks that can run concurrently */
  private static final String CONCURENT_SNAPSHOT_TASKS_KEY = "hbase.snapshot.region.concurrentTasks";
  private static final int DEFAULT_CONCURRENT_SNAPSHOT_TASKS = 3;

  private final ExecutorCompletionService<Void> taskPool;
  private final ThreadPoolExecutor executor;
  private final Aborting abort = new Aborting();

  public SnapshotTaskManager(Server parent, Configuration conf) {
    // configure the executor service
    long keepAlive = conf.getLong(
      RegionServerSnapshotHandler.SNAPSHOT_THEAD_POOL_KEEP_ALIVE_SECONDS,
      RegionServerSnapshotHandler.DEFAULT_SNAPSHOT_THREAD_KEEP_ALIVE);
    int threads = conf.getInt(CONCURENT_SNAPSHOT_TASKS_KEY, DEFAULT_CONCURRENT_SNAPSHOT_TASKS);
    executor = new ThreadPoolExecutor(1, threads, keepAlive, TimeUnit.SECONDS,
        new LinkedBlockingQueue<Runnable>(), new DaemonThreadFactory("rs("
            + parent.getServerName().toString() + ")-snapshot-pool"));
    taskPool = new ExecutorCompletionService<Void>(executor);
  }

  /**
   * Submit a task to the pool. For speed, only 1 caller of this method is allowed, letting us avoid
   * locking to increment the counter
   */
  protected void submitTask(final Runnable task, final CountDownLatch completedLatch) {
    this.submitTask(Executors.callable(task), completedLatch);
  }

  protected void submitTask(final Callable<?> task, final CountDownLatch completedLatch) {
    this.taskPool.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        try {
          task.call();
        } finally {
          completedLatch.countDown();
        }
        return null;
      }
    });
  }

  /**
   * Attempt to cleanly shutdown any running tasks - allows currently running tasks to cleanly
   * finish
   */
  @Override
  public void close() {
    executor.shutdown();
  }

  @Override
  public void abort(String why, Throwable e) {
    if (abort.isAborted()) return;
    abort.abort(why, e);
    this.executor.shutdownNow();
  }

  @Override
  public boolean isAborted() {
    return abort.isAborted();
  }
}