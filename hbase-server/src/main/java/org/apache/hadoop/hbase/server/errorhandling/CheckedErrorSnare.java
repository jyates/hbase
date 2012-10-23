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
package org.apache.hadoop.hbase.server.errorhandling;

/**
 * An {@link ExceptionSnare} that throws a specific checked exception, rather than a general
 * {@link Exception} when checking {@link #failOnException()}.
 * @param <E> type of {@link Exception} to throw
 */
public abstract class CheckedErrorSnare<E extends Exception> extends ExceptionSnare {

  @Override
  public void failOnException() throws E {
    try{
      super.failOnException();
    } catch (Exception e) {
      throw this.map(e);
    }
  }

  /**
   * Map from the generic error type to the specific error
   * <p>
   * Expected to be idemponent.
   * @param e exception found via {@link #handleNotification(javax.management.Notification, Object)}
   * @return a specific exception instance from the general {@link Exception}.
   */
  protected abstract E map(Exception e);
}
