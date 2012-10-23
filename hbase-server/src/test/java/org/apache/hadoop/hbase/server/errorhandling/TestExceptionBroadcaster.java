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

import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationListener;

import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionBroadcaster;
import org.apache.hadoop.hbase.server.errorhandling.notification.AbortNotification;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * Test that we propagate errors through an orchestrator as expected
 */
@Category(SmallTests.class)
public class TestExceptionBroadcaster {

  @Test
  public void testErrorPropagation() throws Exception {

    NotificationListener listener1 = Mockito.mock(NotificationListener.class);
    NotificationListener listener2 = Mockito.mock(NotificationListener.class);

    ExceptionBroadcaster caster = new ExceptionBroadcaster();

    // add the listeners
    caster.addNotificationListener(listener1, null, null);
    caster.addNotificationListener(listener2, null, null);

    // create an artificial error
    String message = "Some error";
    Exception e = new ExceptionForTesting("error");

    caster.receiveException("test", this, e, message);

    // make sure the listeners got the error
    Mockito.verify(listener1, Mockito.times(1)).handleNotification(Mockito.any(Notification.class),
      Mockito.isNull());
    Mockito.verify(listener2, Mockito.times(1)).handleNotification(Mockito.any(Notification.class),
      Mockito.isNull());
    Mockito.reset(listener1, listener2);

    // push another error, which should be passed to listeners
    message = "another error";
    e = new ExceptionForTesting("hello");
    caster.receiveException("test", this, e, message);
    Mockito.verify(listener1, Mockito.times(1)).handleNotification(Mockito.any(Notification.class),
      Mockito.isNull());
    Mockito.verify(listener2, Mockito.times(1)).handleNotification(Mockito.any(Notification.class),
      Mockito.isNull());
    Mockito.reset(listener1, listener2);

    // now create a timer and bind it to the broadcaster
    NotificationBroadcasterSupport other = new NotificationBroadcasterSupport();
    other.addNotificationListener(caster, null, null);

    // send a notification
    new AbortNotification.Builder(other).setCause("Test aborint", e).setSource(this).send();

    // make sure that we got the error
    Mockito.verify(listener1, Mockito.times(1)).handleNotification(Mockito.any(Notification.class),
      Mockito.isNull());
    Mockito.verify(listener2, Mockito.times(1)).handleNotification(Mockito.any(Notification.class),
      Mockito.isNull());
  }
}