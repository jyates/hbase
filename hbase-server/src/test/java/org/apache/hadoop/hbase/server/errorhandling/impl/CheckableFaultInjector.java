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
package org.apache.hadoop.hbase.server.errorhandling.impl;

import org.apache.hadoop.hbase.util.Pair;

/**
 * A {@link PoliciedFaultInjector} which can be <b>statically</b> checked via {@link #getFaulted()}
 * checked to determine if the injector has attempted to inject a fault.
 * @param <E> Exception type to pass back to the error handling
 */
public class CheckableFaultInjector<E extends Exception> extends PoliciedFaultInjector<E> {

  private static boolean faulted;

  /**
   * @param policy
   */
  public CheckableFaultInjector(FaultInjectionPolicy policy) {
    super(policy);
  }

  public static boolean getFaulted() {
    return faulted;
  }

  public static void reset() {
    faulted = false;
  }

  @Override
  protected Pair<E, Object[]> getInjectedError(StackTraceElement[] trace) {
    faulted = true;
    return null;
  }

}
