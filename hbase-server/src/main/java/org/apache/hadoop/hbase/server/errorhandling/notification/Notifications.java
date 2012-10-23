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
import javax.management.NotificationBroadcaster;

/**
 * General Utility class for adding/watching {@link Notification Notifications}.
 * @see TimeoutNotification
 * @see AbortNotification
 */
public class Notifications {
  /**
   * Notification type key for timeout notifications. See {@link TimeoutNotification} for helpers to
   * send/watch these notifications
   */
  public static final String HBASE_TIMEOUT_KEY = "hbase.timeout";

  /**
   * Notification type key for abort notifications. See {@link AbortNotification} for helpers to
   * send/watch these notifications
   */
  public static final String HBASE_ABORT_KEY = "hbase.abort";

  public static void addNotificationListener(NotificationBroadcaster caster, NotificationWatcher watcher){
    caster.addNotificationListener(watcher, watcher, null);
  }
  
  public static void addNotificationListener(NotificationBroadcaster caster,
      NotificationWatcher watcher, Object handback) {
    caster.addNotificationListener(watcher, watcher, handback);
  }


  /**
   * General {@link NotificationWatcher} that just accepts all notifications.
   * <p>
   * Helper utility that be used with
   * {@link Notifications#addNotificationListener(NotificationBroadcaster, NotificationWatcher)} or
   * {@link Notifications#addNotificationListener(NotificationBroadcaster, NotificationWatcher, Object)
   * }
   * .
   */
  @SuppressWarnings("serial")
  public static abstract class AcceptAllNotificationsWatcher implements NotificationWatcher {
    @Override
    public boolean isNotificationEnabled(Notification notification) {
      return true;
    }
  }
}