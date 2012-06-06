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
package org.apache.hadoop.hbase.regionserver.snapshot.monitor;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.regionserver.snapshot.BoundSnapshotFailureListener;
import org.apache.hadoop.hbase.regionserver.snapshot.SnapshotFailureListener;
import org.apache.hadoop.hbase.regionserver.snapshot.SnapshotRequestHandler;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;

/**
 * Factory that produces failure monitors for a given snapshot request
 */
public class FailureMonitorFactory {

  private SnapshotFailureListenable externalError;
  private List<SnapshotFaultInjector> faults = new ArrayList<SnapshotFaultInjector>(0);
  private final long maxTime;

  public FailureMonitorFactory(SnapshotFailureListenable externalErrorMonitor, long maxWaitTime) {
    this.externalError = externalErrorMonitor;
    this.maxTime = maxWaitTime;
  }

  /**
   * @return a {@link SnapshotFailureListenable} to monitor the given snapshot
   *         for external and internal errors.
   */
  public RunningSnapshotErrorMonitor getRunningSnapshotFailureMonitor(SnapshotDescriptor snapshot) {
    // if there are fault injectors, then we need to call them
    if (faults.size() > 0) return new InjectedGlobalSnapshotErrorMonitor(snapshot, externalError,
        faults);
    // otherwise we just use the regular error monitor
    return new GlobalSnapshotErrorMonitor(snapshot, externalError);

  }

  public void addFaultInjector(SnapshotFaultInjector injector) {
    this.faults.add(injector);
  }

  private class GlobalSnapshotErrorMonitor extends BoundSnapshotFailureListener implements
      RunningSnapshotErrorMonitor {
    private final Log LOG = LogFactory.getLog(GlobalSnapshotErrorMonitor.class);
    private final Set<SnapshotErrorMonitor> subtasks = new TreeSet<SnapshotErrorMonitor>();
    private volatile boolean complete = false;
    private volatile boolean failed = false;

    /**
     * @param externalError monitor to check for snapshot failure due to some
     *          external cause
     */
    public GlobalSnapshotErrorMonitor(SnapshotDescriptor snapshot,
        SnapshotFailureListenable externalError) {
      super(snapshot);
      externalError.listenForSnapshotFailure(this);
    }

    /**
     * @param failureMonitor
     * @param now
     * @return
     */
    public SnapshotTimer getTimerErrorMonitor(long now, long wakeFrequency) {
      SnapshotTimer monitor = new SnapshotTimer(snapshot, this, wakeFrequency, now, maxTime);
      return monitor;
    }

    @Override
    public <T> boolean checkForError(Class<T> clazz) {
      // if we are don't, we don't return an error
      if (this.complete) return false;
      // if we failed, we want to return without checking subtasks
      if (this.failed) return true;
      // if the main check failed, check the other monitors
      for (SnapshotErrorMonitor monitor : subtasks) {
        if (monitor.checkForError(clazz)) return true;
      }
      return false;
    }

    @Override
    public void addTaskMonitor(SnapshotErrorMonitor monitor) {
      this.subtasks.add(monitor);
    }

    @Override
    public void complete() {
      this.complete = true;
    }

    @Override
    public void snapshotFailure(String description) {
      LOG.debug("Failing snapshot because: " + description);
      this.failed = true;
    }



  }

  private class InjectedGlobalSnapshotErrorMonitor extends GlobalSnapshotErrorMonitor {
    
    /**
     * @param externalError
     * @param faults
     */
    public InjectedGlobalSnapshotErrorMonitor(SnapshotDescriptor snapshot,
        SnapshotFailureListenable externalError,
        List<SnapshotFaultInjector> faults) {
      super(snapshot, externalError);
    }

  }

  /**
   * Error monitor that will notify the injection handlers if there is a request
   * for an error check
   */
  private class InjectableErrorMonitor implements SnapshotErrorMonitor {
    private final SnapshotErrorMonitor monitor;
    private final List<SnapshotFaultInjector> injectors;

    public InjectableErrorMonitor(SnapshotErrorMonitor monitor,
        List<SnapshotFaultInjector> injectors) {
      this.monitor = monitor;
      this.injectors = injectors;
    }

    @Override
    public <T> boolean checkForError(Class<T> clazz) {
      for(SnapshotFaultInjector injector: injectors)
        if (injector.injectFault(clazz))
          return true;
      return this.monitor.checkForError(clazz);
      }
  }
}
