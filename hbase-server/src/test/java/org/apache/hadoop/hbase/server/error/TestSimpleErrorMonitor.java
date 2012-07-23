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
package org.apache.hadoop.hbase.server.error;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Basic tests of error propagation and handling for a basic situation
 */
@Category(SmallTests.class)
public class TestSimpleErrorMonitor {

  private static final Log LOG = LogFactory.getLog(TestSimpleErrorMonitor.class);
  // always accept the error
  private static final ReceiveErrorChecker alwaysRecieve = new AlwaysReceiveErrorChecker();

  @Test
  public void testErrorPropagation() {

    SimpleErrorListener listener1 = new SimpleErrorListener();
    SimpleErrorListener listener2 = new SimpleErrorListener();

    // just pass through the listener
    ErrorListenerBridge<Object> transformer = new ErrorListenerBridge<Object>() {
      @Override
      public ErrorListener wrap(Object delegate) {
        if (delegate instanceof SimpleErrorListener) return (ErrorListener) delegate;
        return null;
      }
    };

    // just create a generic error
    ErrorFailureThrower<TestingException> thrower = new ErrorFailureThrower<TestingException>() {
      @Override
      public TestingException convertToException(String message, Object... errorInfo) {
        return new TestingException("Exception!");
      }
    };

    BaseBoundErrorMonitor<Object, TestingException> monitor = new BaseBoundErrorMonitor<Object, TestingException>(
        transformer, alwaysRecieve, thrower);

    // add the listeners
    monitor.addErrorListener(listener1);
    monitor.addErrorListener(listener2);

    // create an artificial error
    monitor.receiveError("Some error");

    // make sure the listeners got the error
    assertTrue("Listener1 did't get an error", listener1.error);
    assertEquals("Listener1 had some error info:" + listener1.info, 0, listener1.info.length);
    assertTrue("Listener2 did't get an error", listener2.error);
    assertEquals("Listener2 had some error info:" + listener2.info, 0, listener2.info.length);

    // make sure that we get an exception
    try {
      monitor.failOnError();
      fail("Monitor should have thrown an exception if it fails.");
    } catch (TestingException e) {
      LOG.debug("Got the testing exception!");
    }
    // push another error, but this shouldn't be passed to the listeners
    monitor.receiveError("another error", "hello");
    // make sure we don't re-propagate the error
    assertTrue("Listener1 did't get an error", listener1.error);
    assertEquals("Listener1 had some error info:" + listener1.info, 0, listener1.info.length);
    assertTrue("Listener2 did't get an error", listener2.error);
    assertEquals("Listener2 had some error info:" + listener2.info, 0, listener2.info.length);
  }

  @Test
  public void testErrorConversion() {
    ErrorReceiver listener1 = new ErrorReceiver();
    ErrorReceiver listener2 = new ErrorReceiver();

    // always accept the error
    ReceiveErrorChecker alwaysRecieve = new ReceiveErrorChecker() {
      @Override
      public boolean shouldRecieveError(Object... info) {
        return true;
      }
    };

    // just create a generic error
    ErrorFailureThrower<TestingException> thrower = new ExceptionForTestsThrower();

    BaseBoundErrorMonitor<ErrorReceiver, TestingException> monitor = new BaseBoundErrorMonitor<ErrorReceiver, TestingException>(
        new ErrorToStringConverter(), alwaysRecieve, thrower);

    // bind the listeners to the monitor
    monitor.addErrorListener(listener1);
    monitor.addErrorListener(listener2);

    // create an artificial error
    monitor.receiveError("Some error", "some information");

    // make sure the listeners got the error
    assertNotNull("Listener1 did't get an error", listener1.info);
    assertNotNull("Listener2 did't get an error", listener2.info);

    // make sure that we get an exception
    try {
      monitor.failOnError();
      fail("Monitor should have thrown an exception if it fails.");
    } catch (TestingException e) {
      LOG.debug("Got the testing exception!");
    }
  }
}