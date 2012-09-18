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
package org.apache.hadoop.hbase.regionserver.snapshot;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription.Type;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.snapshot.operation.SnapshotTaskManager;
import org.apache.hadoop.hbase.server.Aborting;
import org.apache.hadoop.hbase.server.commit.ThreePhaseCommit;
import org.apache.hadoop.hbase.server.commit.distributed.DistributedCommitException;
import org.apache.hadoop.hbase.server.commit.distributed.DistributedThreePhaseCommitErrorDispatcher;
import org.apache.hadoop.hbase.server.commit.distributed.DistributedThreePhaseCommitErrorListener;
import org.apache.hadoop.hbase.server.commit.distributed.cohort.CohortMemberTaskBuilder;
import org.apache.hadoop.hbase.server.commit.distributed.cohort.DistributedCommitCohortMemberController;
import org.apache.hadoop.hbase.server.commit.distributed.cohort.DistributedThreePhaseCommitCohortMember;
import org.apache.hadoop.hbase.server.commit.distributed.zookeeper.ZKTwoPhaseCommitCohortMemberController;
import org.apache.hadoop.hbase.server.snapshot.error.SnapshotErrorMonitorFactory;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Handle all the detail work of dealing with snapshots for a {@link HRegionServer}.
 * <p>
 * If one of the snapshots indicates that we should abort, all the current snapshots are failed and
 * we require rolling the cluster to make snapshots available again (something has gone really
 * wrong).
 * <p>
 * On startup, requires {@link #start()} to be called.
 * <p>
 * On shutdown, requires {@link #close()} to be called
 */
public class RegionServerSnapshotHandler extends Configured implements Abortable, Closeable {

  private static final Log LOG = LogFactory.getLog(RegionServerSnapshotHandler.class);

  /** Conf key for number of request threads to start snapshots on regionservers */
  public static final String SNAPSHOT_REQUEST_THREADS = "hbase.snapshot.region.pool.threads";
  /**
   * Parallelization factor when starting snapshots on region servers. Should be close to the number
   * of regions per server.
   */
  public static final int DEFAULT_SNAPSHOT_REQUEST_THREADS = 10;

  /** Conf key for max time to keep threads in snapshot request pool waiting */
  public static final String SNAPSHOT_THEAD_POOL_KEEP_ALIVE_SECONDS = "hbase.snapshot.region.pool.keepalive";
  /** Keep threads alive in request pool for max of 60 seconds */
  public static final long DEFAULT_SNAPSHOT_THREAD_KEEP_ALIVE = 60;

  /** Conf key for frequency (ms) the handler should check to see if snapshot completed */
  public static final String SNAPSHOT_REQUEST_WAKE_FREQUENCY_KEY = "hbase.snapshot.region.wakefrequency";
  /** Default amount of time to check for errors while regions finish snapshotting */
  private static final long DEFAULT_WAKE_FREQUENCY = 5000;

  private final HRegionServer parent;

  private DistributedCommitCohortMemberController snapshotController;

  private DistributedThreePhaseCommitCohortMember cohortMember;
  private Abortable abortable = new Aborting();
  private long wakeFrequency;
  private long globalSnapshotTimeout;
  private long timestampSnapshotTimeout;
  private final SnapshotTaskManager taskManager;

  private SnapshotErrorMonitorFactory snapshotErrorMonitorFactory;

  /**
   * Exposed for testing.
   * @param conf
   * @param parent parent running the snapshot handler
   * @param controller use a custom snapshot controller
   * @param cohortMember use a custom cohort member
   */
  public RegionServerSnapshotHandler(Configuration conf, HRegionServer parent,
      DistributedCommitCohortMemberController controller,
      DistributedThreePhaseCommitCohortMember cohortMember) {
    super(conf);
    this.parent = parent;
    this.snapshotController = controller;
    this.cohortMember = cohortMember;
    readInConfigurationProperties(conf);
    taskManager = new SnapshotTaskManager(parent, conf);
    snapshotErrorMonitorFactory = new SnapshotErrorMonitorFactory();
  }

  /**
   * Create a default snapshot handler - uses a zookeeper based cohort controller.
   * @param conf configuration to use for extracting information like thread pool properties and
   *          frequency to check for errors (wake frequency).
   * @param zkw connection to the zookeeper cluster - <b>NOT</b> managed by <tt>this</tt>
   * @param parent server running the handler
   * @throws KeeperException if the zookeeper cluster cannot be reached
   */
  public RegionServerSnapshotHandler(Configuration conf, ZooKeeperWatcher zkw, HRegionServer parent)
      throws KeeperException {
    super(conf);
    this.parent = parent;
    String nodeName = parent.getServerName().toString();
    this.snapshotController = new ZKTwoPhaseCommitCohortMemberController(zkw,
        HConstants.ONLINE_SNAPSHOT_CONTROLLER_DESCRIPTION, nodeName);

    readInConfigurationProperties(conf);
    long keepAlive = conf.getLong(SNAPSHOT_THEAD_POOL_KEEP_ALIVE_SECONDS,
      DEFAULT_SNAPSHOT_THREAD_KEEP_ALIVE);
    int opThreads = conf.getInt(SNAPSHOT_REQUEST_THREADS, DEFAULT_SNAPSHOT_REQUEST_THREADS);
    // create the actual cohort member
    this.cohortMember = new DistributedThreePhaseCommitCohortMember(wakeFrequency, keepAlive,
        opThreads, snapshotController, new SnasphotCohortMemberBuilder(), nodeName);

    // setup the task manager to run all the snapshots tasks
    taskManager = new SnapshotTaskManager(parent, conf);

    snapshotErrorMonitorFactory = new SnapshotErrorMonitorFactory();
  }

  private void readInConfigurationProperties(Configuration conf) {
    // read in the snapshot request configuration properties
    wakeFrequency = conf.getLong(SNAPSHOT_REQUEST_WAKE_FREQUENCY_KEY, DEFAULT_WAKE_FREQUENCY);
    globalSnapshotTimeout = SnapshotDescriptionUtils.getMaxRegionTimeout(conf, Type.GLOBAL);
    timestampSnapshotTimeout = SnapshotDescriptionUtils.getMaxRegionTimeout(conf, Type.TIMESTAMP);
  }
  /**
   * Start accepting snapshot requests.
   */
  public void start() {
    this.snapshotController.start(cohortMember);
  }

  /**
   * Close <tt>this</tt> and all running snapshot tasks
   * @param force forcefully stop all running tasks
   * @throws IOException
   */
  public void close(boolean force) throws IOException {
    if (force) {
      this.abort("Forcefully closing  - all tasks must stop immediately.", null);
      return;
    }
    // otherwise, let tasks end gracefully
    this.close();
  }

  @Override
  public void close() throws IOException {
    IOException exception = null;
    try {
      this.snapshotController.close();
    } catch (IOException e) {
      exception = e;
    }
    this.taskManager.close();
    this.cohortMember.close();
    if (exception != null) throw exception;
  }

  @Override
  public void abort(String why, Throwable e) {
    abortable.abort(why, e);
    this.taskManager.abort(why, e);
    this.cohortMember.close();
    try {
      this.snapshotController.close();
    } catch (IOException e1) {
      LOG.error("Failed to cleanly close snapshot controller", e1);
    }
  }

  @Override
  public boolean isAborted() {
    return abortable.isAborted();
  }

  /**
   * Build the actual snapshot runner that will do all the 'hard' work
   */
  public class SnasphotCohortMemberBuilder implements CohortMemberTaskBuilder {

    @Override
    public ThreePhaseCommit<? extends DistributedThreePhaseCommitErrorListener, DistributedCommitException> buildNewOperation(
        String name, byte[] data) {
      try {
        // don't run a snapshot if the parent is stop(ping)
        if (parent.isStopping() || parent.isStopped()) {
          throw new IllegalStateException("Can't start snapshot on RS: " + parent.getServerName()
              + ", because stopping/stopped!");
        }

        // don't run a new snapshot if we have aborted
        if (abortable.isAborted()) {
          throw new IllegalStateException("Not starting new snapshot becuase aborting.");
        }

        // unwrap the snapshot information
        SnapshotDescription snapshot = SnapshotDescription.parseFrom(data);

        // check to see if this server is hosting any regions for the snapshots
        // check to see if we have regions for the snapshot
        List<HRegion> involvedRegions;
        try {
          involvedRegions = shouldHandleNewSnapshot(snapshot);
        } catch (IOException e1) {
          throw new IllegalStateException("Failed to figure out if we should handle a snapshot - "
              + "something has gone awry with the online regions.", e1);
        }
        // if we aren't involved, don't run an operation
        if (involvedRegions == null || involvedRegions.size() == 0) return null;

        LOG.debug("Attempting to build new snapshot for: " + snapshot);
        DistributedThreePhaseCommitErrorDispatcher errorDispatcher = new DistributedThreePhaseCommitErrorDispatcher();
        switch (snapshot.getType()) {
        case GLOBAL:
          throw new IllegalArgumentException("Unimpememted snapshot type:" + snapshot.getType());
        case TIMESTAMP:
          throw new IllegalArgumentException("Unimpememted snapshot type:" + snapshot.getType());
        default:
          throw new IllegalArgumentException("Unrecognized snapshot type:" + snapshot.getType());
        }
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalArgumentException("Could not read snapshot information from request.");
      }
    }
  }

  /**
   * Determine if the snapshot should be handled on this server
   * @param snapshot
   * @return the list of online regions. Empty list is returned if no regions are responsible for
   *         the given snapshot.
   * @throws IOException
   */
  private List<HRegion> shouldHandleNewSnapshot(SnapshotDescription snapshot) throws IOException {
    byte[] table = Bytes.toBytes(snapshot.getTable());
    return this.parent.getOnlineRegions(table);
  }
}
