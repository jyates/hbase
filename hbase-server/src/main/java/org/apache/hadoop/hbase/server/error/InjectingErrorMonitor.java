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
package org.apache.hadoop.hbase.server.error;

import java.util.List;

/**
 * Error monitor that delegates for all methods, but wraps error checking to allow the fault
 * injectors to have a chance to inject a fault into the running process
 * @param <E> exception to be thrown on checks of {@link #failOnError()}
 */
public class InjectingErrorMonitor<E extends Exception> implements ErrorMonitor<E> {

  private ErrorMonitor<E> delegate;
  private List<FaultInjector> faults;

  /**
   * Wrap an error monitor with one that will inject faults on calls to {@link #checkForError()}.
   * @param delegate base monitor to wrap
   * @param faults injectors to run each time there is a check for an error
   */
  public InjectingErrorMonitor(ErrorMonitor<E> delegate,
      List<FaultInjector> faults) {
    this.delegate = delegate;
    this.faults = faults;
  }

  @Override
  public void failOnError() throws E {
    this.delegate.failOnError();
  }

  @Override
  public boolean checkForError() {
    // if there are fault injectors, run them
    if (faults.size() > 0) {
      // get the caller of this method. Should be the direct calling class
      StackTraceElement[] trace = Thread.currentThread().getStackTrace();
      for (FaultInjector injector : faults) {
        Object[] info = injector.injectFault(trace);
        if (info != null) {
          delegate.receiveError("Faul", info);
        }
      }
    }
    return delegate.checkForError();
  }

  @Override
  public void receiveError(String message, Object... info) {
    delegate.receiveError(message, info);
  }
}
