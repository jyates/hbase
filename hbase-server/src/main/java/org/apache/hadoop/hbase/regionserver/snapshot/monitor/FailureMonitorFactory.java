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

import org.apache.hadoop.hbase.regionserver.snapshot.SnapshotRequestHandler;

/**
 * Factory that produces failure monitors for a given snapshot request
 */
public class FailureMonitorFactory {

  private SnapshotErrorMonitor externalError;
  private List<SnapshotFaultInjector> faults = new ArrayList<SnapshotFaultInjector>(0);
  private final long maxTime;

  public FailureMonitorFactory(SnapshotErrorMonitor externalErrorMonitor, long maxWaitTime) {
    this.externalError = externalErrorMonitor;
    this.maxTime = maxWaitTime;
  }

  /**
   * @return a {@link SnapshotErrorMonitor} to monitor the given snapshot for
   *         external and internal errors.
   */
  public RunningSnapshotErrorMonitor getRunningSnapshotFailureMonitor() {
    // if there are fault injectors, then we need to call them
    if (faults.size() > 0) return new InjectedGlobalSnapshotErrorMonitor(externalError, faults);
    // otherwise we just use the regular error monitor
    return new GlobalSnapshotErrorMonitor(externalError);

  }

  // XXX this is all implemented pretty wrong - we don't need to track a bunch
  // of error monitors, we need to track a bunch of failure listeners, but use
  // the same method for seeing who is checking. We can use the failInjection
  // stuff as a failureListener to add that also does the checks for the who is
  // checking. This just means adding a method in SnapshotOperation that you can
  // do for checkForError() that propagates the class down.

  /**
   * @param failureMonitor
   * @param now
   * @return
   */
  public SnapshotTimeoutMonitor<SnapshotRequestHandler> getTimerErrorMonitor(
      RunningSnapshotErrorMonitor failureMonitor, long now) {
    SnapshotTimeoutMonitor monitor = new SnapshotTimeoutMonitor(SnapshotRequestHandler.class, now,
        maxTime);
    failureMonitor.addTaskMonitor(monitor);
    return monitor;
  }

  public void addFaultInjector(SnapshotFaultInjector injector) {
    this.faults.add(injector);
  }

  private class GlobalSnapshotErrorMonitor implements RunningSnapshotErrorMonitor {

    private final Set<SnapshotErrorMonitor> subtasks = new TreeSet<SnapshotErrorMonitor>();
    private volatile boolean complete = false;
    /**
     * @param externalError monitor to check for snapshot failure due to some
     *          external cause
     */
    public GlobalSnapshotErrorMonitor(SnapshotErrorMonitor externalError) {
      this.subtasks.add(externalError);
    }

    @Override
    public boolean checkForError() {
      if (this.complete) return false;
      // then check all the subtasks
      for (SnapshotErrorMonitor monitor : subtasks) {
        if (monitor.checkForError()) return true;
      }
      return false;
    }

    @Override
    public <T> SnapshotErrorMonitor getTaskErrorMontior(Class<T> caller) {
      SnapshotErrorMonitor monitor = new ErrorMonitor<T>(caller);
      this.subtasks.add(monitor);
      return monitor;
    }

    @Override
    public void addTaskMonitor(SnapshotErrorMonitor monitor) {
      this.subtasks.add(monitor);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Class<T> getCaller() {
      return (Class<T>) SnapshotRequestHandler.class;
    }

    @Override
    public void complete() {
      this.complete = true;
    }

  }

  private class InjectedGlobalSnapshotErrorMonitor extends GlobalSnapshotErrorMonitor {
    
    /**
     * @param externalError
     * @param faults
     */
    public  InjectedGlobalSnapshotErrorMonitor(SnapshotErrorMonitor externalError,
        List<SnapshotFaultInjector> faults) {
      super(new InjectableErrorMonitor(externalError, faults));
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
    public boolean checkForError() {
      for(SnapshotFaultInjector injector: injectors)
        if(injector.injectFault(this.monitor.getCaller()))
          return true;
        return this.monitor.checkForError();
      }

    @Override
    public <T> Class<T> getCaller() {
     return monitor.getCaller();
    }
  }
}
