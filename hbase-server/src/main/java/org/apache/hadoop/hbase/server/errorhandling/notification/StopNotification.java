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

import static org.apache.hadoop.hbase.server.errorhandling.notification.Notifications.HBASE_STOP_KEY;

import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Stoppable;

/**
 * Helper class for watching/sending timeout-keyed ({@link Notifications#HBASE_ABORT_KEY})
 * notifications.
 * <p>
 * You can send abort notifications via the {@link Builder}. You can then watch for all those
 * notifications via the {@link Watcher}.
 */
public class StopNotification {

  private StopNotification() {
    // private ctor utility classes
  }

  /**
   * Builder for stop notifications. This is a helper to the {@link Stoppable} interface
   */
  public static class Builder extends ExceptionNotificationBuilder {

    /**
     * @param caster
     */
    public Builder(NotificationBroadcasterSupport caster) {
      super(HBASE_STOP_KEY, caster);
    }
  }

  /**
   * Helper class that only accepts notifications that are HBase 'abort' notifications. These can be
   * built via {@link Builder}.
   */
  @SuppressWarnings("serial")
  public abstract static class Watcher implements NotificationWatcher {

    @Override
    public boolean isNotificationEnabled(Notification notification) {
      return notification.getType().equals(HBASE_STOP_KEY);
    }
  }

  /**
   * Stop a {@link Stoppable} when is receives a 'stop' notification (either of from a
   * {@link Builder StopNotification#Builder} or typed {@link Notifications#HBASE_STOP_KEY}).
   */
  @SuppressWarnings("serial")
  public static class Stopper extends Watcher {
    private static final Log LOG = LogFactory.getLog(Stopper.class);
    private Stoppable stop;

    public Stopper(Stoppable stop) {
      this.stop = stop;
    }

    @Override
    public void handleNotification(Notification notification, Object handback) {
      LOG.debug("Got a stop notification: " + notification + ", stopping:" + stop);
      stop.stop(notification.getMessage());
    }
  }
}
