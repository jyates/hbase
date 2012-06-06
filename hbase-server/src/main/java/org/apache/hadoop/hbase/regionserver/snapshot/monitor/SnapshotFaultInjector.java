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

/**
 * Fault injector to be used in testing for injecting faults into running a
 * snapshot.
 * <p>
 * This class will be called whenever a class checks for snapshot failure.
 * <p>
 * They can be added to a {@link RunningSnapshotErrorMonitor} via the
 * {@link FailureMonitorFactory#addFaultInjector(SnapshotFaultInjector)}
 * <b>before</b> the snapshot is started.
 */
public interface SnapshotFaultInjector {

  /**
   * Called by the specified class whenever checking for snapshot failure.
   * @param clazz class checking for a snapshot error
   * @return <tt>true</tt> if a fault should be propagated to the rest of the
   *         snapshot process, <tt>false</tt> otherwise.
   */
  @SuppressWarnings("rawtypes")
  public boolean injectFault(Class clazz);
}
