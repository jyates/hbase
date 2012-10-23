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

import static org.apache.hadoop.hbase.server.errorhandling.notification.Notifications.HBASE_ABORT_KEY;

import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;

import org.apache.hadoop.hbase.Abortable;

/**
 * Helper class for watching/sending timeout-keyed ({@link Notifications#HBASE_ABORT_KEY})
 * notifications.
 * <p>
 * You can send abort notifications via the {@link Builder}. You can then watch for all those
 * notifications via the {@link Watcher}.
 */
public class AbortNotification {

  /**
   * Builder for abort notifications. This is a helper to the {@link Abortable} interface
   * <p>
   * Does not support:
   * <ol>
   * <li>{@link #setData(Object)}
   * <ul>
   * <li>Use {@link #setCause(String, Throwable)} instead</li>
   * </ul>
   * </li>
   * <li> {@link #setMessage(String)}
   * <ul>
   * <li>Use {@link #setCause(String, Throwable)} instead</li>
   * </ul>
   * </li>
   */
  public static class Builder extends ExceptionNotificationBuilder {

    /**
     * @param caster
     */
    public Builder(NotificationBroadcasterSupport caster) {
      super(HBASE_ABORT_KEY, caster);
    }

    /**
     * set the exception from {@link Abortable#abort(String, Throwable)}.
     * <p>
     * Helper method to replace {@link #setMessage(String)} and {@link #setData(Object)}
     * <p>
     * Retreive data from sent notification via {@link AbortNotification#getReason(Notification)}
     * and {@link AbortNotification#getCause(Notification)}
     * @param msg message describing the why there was an abort
     * @param exception cause of the abort
     * @return <tt>this</tt> for chaining.
     */
    public ExceptionNotificationBuilder setCause(String msg, Throwable exception) {
      super.setMessage(msg);
      return super.setData(exception);
    }

    @Override
    public ExceptionNotificationBuilder setData(Object data) {
      throw new UnsupportedOperationException("Use setCause instead");
    }

    @Override
    public ExceptionNotificationBuilder setMessage(String message) {
      throw new UnsupportedOperationException("Use setCause instead");
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
      return notification.getType().equals(HBASE_ABORT_KEY);
    }
  }

  public static Exception getCause(Notification cause) {
    return (Exception) cause.getUserData();
  }

  public static String getReason(Notification cause) {
    return cause.getMessage();
  }
}
