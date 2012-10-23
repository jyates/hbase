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
import javax.management.NotificationBroadcasterSupport;

import com.google.common.base.Preconditions;

/**
 * Generic builder for hbase-specific exception notifications.
 * @see AbortNotification#Builder
 */
public class ExceptionNotificationBuilder {

  protected final NotificationBroadcasterSupport caster;
  protected final String key;
  protected Object source;
  protected Object data;
  protected String message;

  protected ExceptionNotificationBuilder(String key, NotificationBroadcasterSupport caster) {
    this.key = key;
    this.caster = caster;
  }

  public ExceptionNotificationBuilder setSource(Object source) {
    this.source = source;
    return this;
  }

  public ExceptionNotificationBuilder setData(Object data) {
    this.data = data;
    return this;
  }

  public ExceptionNotificationBuilder setMessage(String message) {
    this.message = message;
    return this;
  }

  public void send() {
    setParams();
    verify();
    Notification notification = new Notification(key, source, 0, message);
    notification.setUserData(data);
    caster.sendNotification(notification);
  }

  /**
   * Verify that necessary fields are set. Called after {@link #setParams()} in {@link #send()}.
   */
  protected void verify() {
    Preconditions.checkNotNull(this.key, "Must have an error key");
    Preconditions.checkNotNull(this.source, "Must have an error source");
  }

  /**
   * Subclass hook to set internal params. Called in #send() to ensure all necessary properties are
   * set before sending the error notification
   */
  protected void setParams() {
    // noop
  }
}