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
package org.apache.hadoop.hbase.server.errorhandling.notification;

import javax.management.Notification;
import javax.management.NotificationListener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionCheckable;

/**
 * Simple exception handler that keeps track of whether of its failure state, and the exception that
 * should be thrown based on the received error.
 * <p>
 * Ensures that an exception is not propagated if an error has already been received, ensuring that
 * you don't have infinite error propagation.
 * <p>
 * You can think of it like a 'one-time-use' {@link ExceptionCheckable} - once it receives an error
 * will not listen to any new error updates.
 * <p>
 * Thread-safe.
 */
public class ErrorSnare implements NotificationListener, ErrorCheckable {
  private static final Log LOG = LogFactory.getLog(ErrorSnare.class);
  private volatile boolean found;
  private volatile Exception cause;

  @Override
  public synchronized void handleNotification(Notification notification, Object handback) {
    if (this.found) return;
    this.found = true;
    try {
      cause = (Exception) notification.getUserData();
    } catch (ClassCastException e) {
      LOG.error("Got a notification that didn't contain an exception. Skipping");
    }
  }

  @Override
  public boolean checkForException() {
    return this.found;
  }

  @Override
  public void failOnException() throws Exception {
    if (this.cause != null) throw cause;
  }
}
