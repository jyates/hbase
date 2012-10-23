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

import static org.apache.hadoop.hbase.server.errorhandling.notification.Notifications.addNotificationListener;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import javax.management.Notification;
import javax.management.timer.Timer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.server.errorhandling.WeakReferencingNotificationBroadcasterSupport;
import org.apache.hadoop.hbase.server.errorhandling.notification.AbortNotification;
import org.apache.hadoop.hbase.server.errorhandling.notification.NotificationWatcher;
import org.apache.hadoop.hbase.server.errorhandling.notification.Notifications;
import org.apache.hadoop.hbase.server.errorhandling.notification.TimeoutNotification;
import org.junit.Test;

/**
 * 
 */
public class TestNewExceptionHandling {

  private static final Log LOG = LogFactory.getLog(TestNewExceptionHandling.class);

  @Test
  @SuppressWarnings("serial")
  public void test() throws Exception{
    
    final boolean[] listeners = new boolean[] { false, false, false };

    WeakReferencingNotificationBroadcasterSupport caster = new WeakReferencingNotificationBroadcasterSupport();
    NotificationWatcher abort = new AbortNotification.Watcher() {
      
      @Override
      public void handleNotification(Notification notification, Object handback) {
        LOG.debug("Abort notification:"+notification);
        listeners[0] = true;
      }
    };
    NotificationWatcher all = new Notifications.AcceptAllNotificationsWatcher() {

      @Override
      public void handleNotification(Notification notification, Object handback) {
        LOG.debug("Accept all notification:" + notification);
        listeners[1] = true;
      }
    };
    NotificationWatcher timeout = new TimeoutNotification.Watcher() {

      @Override
      public void handleNotification(Notification notification, Object handback) {
        LOG.debug("Accept all notification:" + notification);
        listeners[2] = true;
      }
    };

    // bind the watchers to the broadcaster
    addNotificationListener(caster, all);
    addNotificationListener(caster, abort);
    addNotificationListener(caster, timeout);

    // send an abort notification
    new AbortNotification.Builder(caster).setCause("Abort for testing", new Exception())
        .setSource(this).send();
    
    //check the results
    assertTrue(listeners[0]);
    assertTrue(listeners[1]);
    assertFalse(listeners[2]);

    // reset the checking
    listeners[0] = false;
    listeners[1] = false;
    listeners[2] = false;

    // and then send via the timer
    Timer t = new Timer();
    // make sure we send past notifications
    t.setSendPastNotifications(true);
    // and start running the timer
    t.start();
    // bind the watchers to the timer
    addNotificationListener(t, all);
    addNotificationListener(t, abort);
    addNotificationListener(t, timeout);

    // set a message way in the future
    new TimeoutNotification.Builder(t).setDuration(100000).setMessage("Testing future timeout")
        .send();

    assertFalse(listeners[0]);
    assertFalse(listeners[1]);
    assertFalse(listeners[2]);

    // cancel the notification
    t.removeAllNotifications();

    // send one right away
    new TimeoutNotification.Builder(t).setDuration(0).setMessage("Testing timeout").send();
    // make sure it gets there
    Thread.sleep(100);

    // make sure we got the timeout notification
    assertFalse(listeners[0]);
    assertTrue(listeners[1]);
    assertTrue(listeners[2]);
  }
}
