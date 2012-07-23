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
 * Simple class that just delegates all calls for error handling/briding/acceptance to
 * implementations that handle each of the interfaces.
 * @see ErrorListenerBridge
 * @see ErrorFailureThrower
 * @see ReceiveErrorChecker
 */
class DelegatingErrorConverter<T, E> implements ErrorConverter<T, E> {

  private final ErrorListenerBridge<T> transformer;
  private final ErrorFailureThrower<E> thrower;
  private final ReceiveErrorChecker checker;

  /**
   * @param transformer
   * @param thrower
   * @param checker
   */
  public DelegatingErrorConverter(ErrorListenerBridge<T> transformer,
      ErrorFailureThrower<E> thrower, ReceiveErrorChecker checker) {
    this.transformer = transformer;
    this.thrower = thrower;
    this.checker = checker;
  }

  @Override
  public ErrorListener wrap(T delegate) {
    return this.transformer.wrap(delegate);
  }

  @Override
  public E convertToException(String message, Object... errorInfo) {
    return this.thrower.convertToException(null, errorInfo);
  }

  @Override
  public boolean shouldRecieveError(Object... info) {
    return this.checker.shouldRecieveError(info);
  }

}