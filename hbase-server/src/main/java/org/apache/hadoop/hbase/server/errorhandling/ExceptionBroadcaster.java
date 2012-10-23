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

import java.util.concurrent.atomic.AtomicLong;

import javax.management.Notification;
import javax.management.NotificationListener;

/**
 * Listen for and pass along exception notifications.
 * <p>
 * This is a general interface that acts a hub between notification senders and notification
 * listeners.
 * <p>
 * This is an implementation of both {@link javax.management.NotificationEmitter} and
 * {@link NotificationListener}. Any notifications received via
 * {@link #sendNotification(Notification)} - send a notification directly through <tt>this</tt> or
 * via {@link #handleNotification(Notification, Object)} - indirectly when <tt>this</tt> acts as a
 * listener - get passed on to any attached listeners.
 * <p>
 * Any listeners added via
 * {@link #addNotificationListener(NotificationListener, javax.management.NotificationFilter, Object)}
 * are <b>weakly referenced</b>. This is, any reference contained here does not count towards its
 * total reference count. Therefore you must maintain a reference to the object if you want it to
 * receive notifications. The advantage of this is that you do <b>not</b> need to remove the
 * listener when you are done with it - it will be automatically removed when it goes out of scope.
 * @see WeakReferencingNotificationBroadcasterSupport
 */
public class ExceptionBroadcaster extends WeakReferencingNotificationBroadcasterSupport implements
    NotificationListener {
  private final AtomicLong exceptionID = new AtomicLong(0);

  public void receiveException(String type, Object source, Throwable cause, String message) {
    Notification n = new Notification(type, source, exceptionID.getAndIncrement(), message);
    n.setUserData(cause);
    this.sendNotification(n);
  }

  @Override
  public void handleNotification(Notification notification, Object handback) {
    this.sendNotification(notification);
  }
}