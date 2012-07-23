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
 * Transform the generic error to a correctly typed error for each type of process/operation.
 * <p>
 * Each operation will need to define its own {@link ErrorListenerBridge} for the errors it
 * needs to handle, but this lets it ignore (for the most part) how errors are transformed and
 * dispatched, and just deal with the errors themselves.
 * @param <T> Type of error receiver that needs to be updated.
 */
public interface ErrorListenerBridge<T> {

  /**
   * Wrap the delegate so that calls to {@link ErrorListener#receiveError(String, Object...)} get
   * transformed to a call that the delegate can understand.
   * <p>
   * This is essentially a factory method that creates an {@link ErrorListener} for each of the
   * actual receivers.
   * @param delegate end receiver of error notifications.
   * @return an {@link ErrorListener} that can listen for general errors to the operation.
   */
  public ErrorListener wrap(final T delegate);
}
