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
package org.apache.hadoop.hbase.snapshot.monitor;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;

/**
 * Monitor for errors in a snapshot. Only fails when an error occurs for the snapshot specified for
 * the constructor. This is the most general util and should be used throughout the snapshot for
 * keeping track of all the error monitors.
 * <p>
 * Allows for cyclic loops that eventually call itself without going into an infinite recursion, to
 * make it easier to not worry about which object is listening to which other object in a snapshot,
 * allowing a full graph of state checking
 */
public class SnapshotErrorCheckable implements SnapshotFailureMonitor, SnapshotFailureListenable {
  private static final Log LOG = LogFactory.getLog(SnapshotErrorCheckable.class);
  private static final List<SnapshotFaultInjector> faults = new ArrayList<SnapshotFaultInjector>(0);
  // propagate errors throughout the snapshot
  protected SnapshotErrorPropagator propagator;

  // snasphot info/state
  protected final SnapshotDescriptor snapshot;
  protected volatile boolean failed = false;
  private String reason;

  /**
   * @param snapshot snapshot on which to monitor failures
   */
  public SnapshotErrorCheckable(SnapshotDescriptor snapshot) {
    this.snapshot = snapshot;
    this.propagator = new SnapshotErrorPropagator();
    // make sure we hear about snapshot failures
    this.propagator.addSnapshotFailureListener(this);
  }

  public SnapshotErrorCheckable(SnapshotDescriptor snapshot, SnapshotFailureListener... listeners) {
    this(snapshot);
    // add the passed in listener
    for (SnapshotFailureListener listener : listeners) {
      if (listener == null) return;
      this.propagator.addSnapshotFailureListener(listener);
    }
  }

  /** @return the snapshot descriptor for this snapshot */
  public SnapshotDescriptor getSnapshot() {
    return this.snapshot;
  }

  public SnapshotErrorPropagator getErrorPropagator() {
    return this.propagator;
  }

  @Override
  public boolean checkForError() {
    // if there are fault injectors, run them
    if (faults.size() > 0) {
      // get the caller of this method. Should be the direct calling class
      StackTraceElement[] trace = Thread.currentThread().getStackTrace();
      for (SnapshotFaultInjector injector : faults)
        if (injector.injectFault(trace)) return true;
    }
    return this.failed;
  }

  public void snapshotFailure(String reason, Exception e) {
    LOG.error("Snapshot failed because " + reason, e);
    this.snapshotFailure(snapshot, reason);
  }

  @Override
  public void failOnError() throws SnapshotCreationException {
    if (this.checkForError()) {
      LOG.error("Snapshot failure found!");
      throw new SnapshotCreationException("Found an error while running the snapshot:" + reason);
    }
  }

  @Override
  public synchronized final void snapshotFailure(SnapshotDescriptor snapshot, String description) {
    LOG.error("Got a snapshot failure notification!");
    // if alreday failed or we don't care, just return
    if (failed || !this.snapshot.equals(snapshot)) return;
    this.failed = true;
    this.reason = description;
    LOG.error("Propagating snapshot failure notification");
    // propagate the error down
    this.propagator.snapshotFailure(snapshot, description);
  }

  @Override
  public void addSnapshotFailureListener(SnapshotFailureListener failable) {
    if (failable == null) return;
    propagator.addSnapshotFailureListener(failable);
  }

  /**
   * Add a fault injector
   * <p>
   * Exposed for TESTING!
   * @param injector injector to add
   */
  public static void addFaultInjector(SnapshotFaultInjector injector) {
    faults.add(injector);
  }

  /**
   * Remove any existing fault injectors.
   * <p>
   * Exposed for TESTING!
   */
  public static void clearFaultInjectors() {
    faults.clear();
  }
}