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
 * Simple error converter that just converts the first info element into a string that is based to
 * the delegate
 */
public class ErrorToStringConverter implements ErrorListenerBridge<ErrorReceiver> {
  @Override
  public ErrorListener wrap(final ErrorReceiver delegate) {
    return new ErrorListener(){
      @Override
      public void receiveError(String message, Object... info) {
        delegate.receiveError(message, (String) info[0]);
      }
    };
  } 
}