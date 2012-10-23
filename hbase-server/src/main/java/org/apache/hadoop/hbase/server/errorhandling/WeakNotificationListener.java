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

import java.lang.ref.WeakReference;

import javax.management.ListenerNotFoundException;
import javax.management.Notification;
import javax.management.NotificationBroadcaster;
import javax.management.NotificationListener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * {@link NotificationListener} that is weakly bound to a <b>single</b>
 * {@link NotificationBroadcaster}. If the internal references to a {@link NotificationListener} is
 * no longer been reached (it has been gc'ed), then <tt>this</tt> is removed from the parent's list
 * of notification listeners.
 */
class WeakNotificationListener extends WeakReference<NotificationListener> implements
    NotificationListener {

  private static final Log LOG = LogFactory.getLog(WeakNotificationListener.class);

  /**
   * Parent that is storing this listener.
   */
  private NotificationBroadcaster parent;

  /**
   * @param referent
   * @param parent
   */
  public WeakNotificationListener(NotificationListener referent, NotificationBroadcaster parent) {
    super(referent);
    this.parent = parent;
  }

  @Override
  public void handleNotification(Notification notification, Object handback) {
    NotificationListener ref = this.get();
    // remove the this if the reference has been removed
    if (ref == null) {
      try {
        parent.removeNotificationListener(this);
      } catch (ListenerNotFoundException e) {
        LOG.warn("This not found when attempting to remvoe from list of bound listeners, ignoring",
          e);
      }
      return;
    }

    // the reference still exists, so we pass on the notification
    ref.handleNotification(notification, handback);
  }
}