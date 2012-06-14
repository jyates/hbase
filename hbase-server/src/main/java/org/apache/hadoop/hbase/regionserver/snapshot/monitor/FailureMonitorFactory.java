/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver.snapshot.monitor;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;

/**
 * Factory that produces failure monitors for a given snapshot request
 */
public class FailureMonitorFactory {

  private List<SnapshotFaultInjector> faults = new ArrayList<SnapshotFaultInjector>(0);
  private final long maxTime;

  public FailureMonitorFactory(long maxWaitTime) {
    this.maxTime = maxWaitTime;
  }

  /**
   * Get a monitor for the given snapshot. Ensures external errors propagte down
   * and internal errors propagate out
   * @param snapshot to be monitored
   * @param externalError external listener for a snapshot error
   * @return a {@link SnapshotFailureListenable} to monitor the given snapshot
   *         for external and internal errors.
   */
  public RunningSnapshotFailureMonitor getRunningSnapshotFailureMonitor(
      SnapshotDescriptor snapshot, SnapshotFailureListenable externalError) {
    RunningSnapshotFailureMonitor errorMonitor;
    // if there are fault injectors, then we need to create a special monitor
    if (faults.size() > 0) {
      errorMonitor = new InjectedGlobalSnapshotErrorMonitor(snapshot, faults);
    } else {
      // otherwise we just use the regular error monitor
      errorMonitor = new GlobalSnapshotErrorMonitor(snapshot);
    }
    // start monitoring for errors to this snapshot
    externalError.listenForSnapshotFailure(errorMonitor);
    return errorMonitor;
  }

  public void addFaultInjector(SnapshotFaultInjector injector) {
    this.faults.add(injector);
  }

  private class GlobalSnapshotErrorMonitor extends RunningSnapshotFailureMonitor {
    private final Log LOG = LogFactory.getLog(GlobalSnapshotErrorMonitor.class);
    private volatile boolean failed = false;

    /**
     * @param snapshot snapshot that will be monitored
     */
    public GlobalSnapshotErrorMonitor(SnapshotDescriptor snapshot) {
      super(snapshot);
    }

    @Override
    public SnapshotTimer getTimerErrorMonitor(long now, long wakeFrequency) {
      SnapshotTimer monitor = new SnapshotTimer(snapshot, this, wakeFrequency, now, maxTime);
      return monitor;
    }

    @Override
    public boolean checkForError() {
      // if we failed, we want to return without checking subtasks
      if (this.failed) return true;
      return false;
    }

    @Override
    public void snapshotFailure(String description) {
      LOG.debug("Failing snapshot because: " + description);
      this.failed = true;
    }
  }

  /**
   * A {@link RunningSnapshotFailureMonitor} that will allow faults to be
   * injected into the running snapshot for testing
   */
  private class InjectedGlobalSnapshotErrorMonitor extends GlobalSnapshotErrorMonitor {
    private final Log LOG = LogFactory.getLog(InjectedGlobalSnapshotErrorMonitor.class);
    private final List<SnapshotFaultInjector> injectors;

    /**
     * Create the error monitor to call the fault injectors whenever a caller
     * invokes {@link #checkForError()}
     * @param snapshot snapshot to monitor
     * @param faults fault injectors to invoke
     */
    public InjectedGlobalSnapshotErrorMonitor(SnapshotDescriptor snapshot,
        List<SnapshotFaultInjector> faults) {
      super(snapshot);
      this.injectors = faults;
    }

    @Override
    public boolean checkForError() {
      // get the caller of this method. Should be the direct calling class
      // 0 = this, 1 = caller
      StackTraceElement elem = new Throwable().fillInStackTrace().getStackTrace()[1];
      Class<?> clazz;
      try {
        clazz = Class.forName(elem.getClassName());
        for (SnapshotFaultInjector injector : injectors)
          if (injector.injectFault(clazz)) return true;
      } catch (ClassNotFoundException e) {
        LOG.debug("Couldn't load class from the stack trace", e);
      }
      return super.checkForError();
    }

  }
}
