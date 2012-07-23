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

/**
 * Tranforms an error received into the correct exception type thrown via
 * {@link ErrorMonitor#failOnError()}. This is a helper for an {@link ErrorMonitor} that creates the
 * correct exception from raw information, akin to the {@link ErrorListenerBridge}.
 * @see ErrorListener
 * @param <T> Exception thrown when checking for error
 */
public interface ErrorFailureThrower<T> {

  /**
   * Convert the error information that was received into an exception that will be throw back to
   * checkers.
 * @param message TODO
 * @param errorInfo information about the error received via
   *          {@link ErrorListener#receiveError(String, Object...)}
   * @return the exception to throw if a process calls {@link ErrorMonitor#failOnError()}
   */
  public T convertToException(String message, Object... errorInfo);

}
