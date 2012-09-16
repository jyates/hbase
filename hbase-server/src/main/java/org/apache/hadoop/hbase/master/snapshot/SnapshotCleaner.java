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
package org.apache.hadoop.hbase.master.snapshot;

import java.io.IOException;
import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Threads;

/**
 * Simple helper thread that async cleans up a snapshot working directory
 */
public class SnapshotCleaner extends Thread {

  private static final Log LOG = LogFactory.getLog(SnapshotCleaner.class);
  /** Conf key for time in seconds to wait for the snapshot when cleaning up (in ms) */
  static final String CLEANUP_SNAPSHOT_WAIT_TIME_KEY = "hbase.snapshot.master.cleanup.waittime";
  private static final long DEFAULT_CLEANUP_SNAPSHOT_WAIT_TIME = 10000;

  private final Path workingDir;
  private final FileSystem fs;
  private final long waitTime;
  /**
   * Object used to wait for timeout to expire. Exposed for testing to signal the cleaner to run on
   * request.
   */
  public static final Object wait = new Object();
  private static final int MAX_RUNNING_CLEANERS = 1;
  public static final Semaphore running = new Semaphore(MAX_RUNNING_CLEANERS, true);
  static volatile SnapshotCleaner current;

  /**
   * Start cleanup of the passed in working directory
   * @param workingDir working directory to cleanup
   * @param fs {@link FileSystem} where the directory lives
   * @param conf configuration to use when getting wait time for the cleaner
   */
  public static void launchSnapshotCleanup(Path workingDir, FileSystem fs, Configuration conf) {
    // create the snapshot, counting on the semaphore to block as necessary
    cleanupSnapshot(workingDir, fs, conf);
    // kickoff the cleaner
    Threads.setDaemonThreadRunning(current, "snapshot-cleanup");
  }

  /**
   * Create the snapshot cleaner for the working directory.
   * <p>
   * <b>Only exposed for testing</b>, instead you should probably use
   * {@link #launchSnapshotCleanup(Path, FileSystem, Configuration)}.
   * @param workingDir working directory to cleanup
   * @param fileSystem {@link FileSystem} where the directory lives
   * @param conf configuration to use when getting wait time for the cleaner
   */
  static void cleanupSnapshot(Path workingDir, FileSystem fileSystem, Configuration conf) {
    synchronized (SnapshotCleaner.class) {
      // if we are currently cleaning a snapshot with the same working dir, then we are done
      SnapshotCleaner cleaner = current;
      if (cleaner != null && cleaner.workingDir.equals(workingDir)) return;
    }

    // wait for a cleaner to become available
    try {
      running.acquire();
    } catch (InterruptedException e) {
      // if we are interrupted, then don't make a new cleaner
      return;
    }

    // create a new cleaner to clean the directory
    current = new SnapshotCleaner(workingDir, fileSystem, conf);
  }

  /**
   * Create a snapshot cleaner to delete the working directory from the {@link FileSystem} in the
   * configured amount of time.
   * @param workingDir directory to delete
   * @param fileSystem {@link FileSystem} where the directory lives
   * @param conf {@link Configuration} to use for the cleanup time
   */
  private SnapshotCleaner(Path workingDir, FileSystem fileSystem, Configuration conf) {
    this.workingDir = workingDir;
    this.fs = fileSystem;
    this.waitTime = conf
        .getLong(CLEANUP_SNAPSHOT_WAIT_TIME_KEY, DEFAULT_CLEANUP_SNAPSHOT_WAIT_TIME);
  }

  @Override
  public void run() {
    LOG.debug("Starting cleanup of snapshot:" + workingDir);
    boolean success = false;
    while (!success) {
      try {
        synchronized (wait) {
          wait.wait(waitTime);
        }
      } catch (InterruptedException e) {
        LOG.debug("Got interrupted while waiting on runNow, ignoring");
        Thread.currentThread().interrupt();
      }
      // cleanup the working directory, if it exists
      LOG.debug("Running cleanup of snapshot:" + workingDir);
      try {
        if (!fs.exists(workingDir)) {
          LOG.debug("Snapshot working dir:" + workingDir + " already deleted.");
          break;
        }
        success = fs.delete(workingDir, true);
        if (!success) {
          if (!fs.exists(workingDir)) {
            LOG.debug("Snapshot working dir: " + workingDir + " was already deleted!");
            break;
          }
          LOG.warn("Failed to delete snapshot working directory:" + workingDir);
        }
      } catch (IOException e) {
        LOG.warn("Couldn't reach HDFS, trying again.");
      }
    }
    running.release();
    // notify any interested that we finished running the current snapshot
    synchronized (running) {
      running.notifyAll();
    }
    current = null;
    LOG.debug("Deleted snapshot working dir: " + workingDir);
  }

  /**
   * Make sure the snapshot cleaner runs
   */
  public static void ensureCleanerRuns() {
    if (current == null || running.availablePermits() == MAX_RUNNING_CLEANERS) {
      LOG.debug("No current snapshot cleaner, done!");
      return;
    }

    // start the cleaner, if it isn't already
    synchronized (wait) {
      wait.notify();
    }

    LOG.debug("Waiting on cleaner to finish");
    try {
      synchronized (running) {
        running.wait();
      }
    } catch (InterruptedException e) {
      LOG.warn("Got interrupted waiting for snapshot cleaner to run!");
      Thread.currentThread().interrupt();
    }
  }
}