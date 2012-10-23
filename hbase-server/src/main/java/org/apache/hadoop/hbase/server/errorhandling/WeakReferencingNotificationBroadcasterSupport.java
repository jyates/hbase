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


import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;

/**
 * A {@link NotificationBroadcasterSupport} that only keeps weak references of the the
 * {@link NotificationListener listeners} bound to <tt>this</tt>. When the reference has been
 * garbage collected, the listener is automatically removed from the list of registered listeners,
 * rather than requiring you to call {@link #removeNotificationListener(NotificationListener)} or
 * {@link #removeNotificationListener(NotificationListener, NotificationFilter, Object)}.
 * <p>
 * Otherwise acts just like a regular {@link NotificationBroadcasterSupport}.
 */
public class WeakReferencingNotificationBroadcasterSupport extends NotificationBroadcasterSupport {

  @Override
  public void addNotificationListener(NotificationListener listener, NotificationFilter filter,
      Object handback) {
    super.addNotificationListener(new WeakNotificationListener(listener, this), filter, handback);
  }
}