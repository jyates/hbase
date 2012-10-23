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

import java.util.Date;

import javax.management.Notification;
import javax.management.timer.Timer;

import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import com.google.common.base.Preconditions;

/**
 * Helper class for watching/sending timeout-keyed ({@link Notifications#HBASE_TIMEOUT_KEY})
 * notifications.
 * <p>
 * Sending timeout notifications is via the {@link Builder}. You can then watch for all those
 * notifications via the {@link Watcher}.
 */
public class TimeoutNotification {

  private TimeoutNotification() {
    // hidden ctor for utility class
  }

  /**
   * Builder for timeout notifications. A timeout occurs after a specific duration (specified via
   * {@link #setDuration(long)} and passes along the data specified in {@link #setData(Object)},
   * with the start time set when {@link #send()} is called.
   * <p>
   * Does not support:
   * <ol>
   * <li>{@link #setSource(Object)}
   * <ul>
   * <li>Source will be the Timer specified in the constructor</li>
   * </ul>
   * </li>
   * <li> {@link #setMessage(String)}
   * <ul>
   * <li>Message specified by duration and current time when send() is called.</li>
   * </ul>
   * </li>
   */
  public static class Builder extends ExceptionNotificationBuilder {
  
    private long start;
    private long duration;
  
    public Builder(Timer source) {
      super(Notifications.HBASE_TIMEOUT_KEY, source);
      super.setSource(source);
    }
  
    public Builder setDuration(long time) {
      this.duration = time;
      return this;
    }
  
    public Builder setData(Object data) {
      super.setData(data);
      return this;
    }

    @Override
    public ExceptionNotificationBuilder setSource(Object source) {
      throw new UnsupportedOperationException(
          "Source will be the Timer specified in the constructor.");
    }

    @Override
    public ExceptionNotificationBuilder setMessage(String message) {
      throw new UnsupportedOperationException(
          "Message specified by duration and current time when send() is called.");
    }

    protected void setParams() {
      this.start = EnvironmentEdgeManager.currentTimeMillis();
      Preconditions.checkArgument(duration >= 0, "Must have a duration >= 0");
      this.message = "Timeout from " + this.source + ". Started:" + this.start + ", with duration:"
          + this.duration;
    }
  
    public void send() {
      setParams();
      verify();
      ((Timer) this.caster).addNotification(key, message, data, new Date(start + duration));
  
    }
  }

  /**
   * Watcher that only accepts notifications with the {@link Notifications#HBASE_TIMEOUT_KEY} key.
   * This will catch all notifications built with {@link Builder}.
   */
  @SuppressWarnings("serial")
  public static abstract class Watcher implements NotificationWatcher {
    @Override
    public boolean isNotificationEnabled(Notification notification) {
      return notification.getType().equals(Notifications.HBASE_TIMEOUT_KEY);
    }
  }
}