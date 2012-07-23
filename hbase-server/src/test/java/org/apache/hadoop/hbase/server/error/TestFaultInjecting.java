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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test that we can correctly inject faults for testing
 */
@Category(SmallTests.class)
public class TestFaultInjecting {

  private static final Log LOG = LogFactory.getLog(TestFaultInjecting.class);

  @Test
  public void testSimpleFaultInjection() {
    ErrorConverter<ErrorReceiver, TestingException> converter = new DelegatingErrorConverter<ErrorReceiver, TestingException>(new ErrorToStringConverter(), new ExceptionForTestsThrower(), new AlwaysReceiveErrorChecker());
    
    ErrorMonitorFactory<ErrorReceiver, TestingException> factory = new ErrorMonitorFactory<ErrorReceiver, TestingException>(converter, null);
    String info = "info";
    ErrorMonitorFactory.addFaultInjector(new StringFaultInjector(info));
    ErrorMonitor<TestingException> monitor = factory.createErrorMonitor();

    // test that we actual inject a fault
    assertTrue(monitor.checkForError());
    try {
      monitor.failOnError();
      fail("Monitor didn't get an build an exception from the fault in the factory.");
    } catch (TestingException e) {
      LOG.debug("Correctly got an exception from the test!");
    }
  }

  /**
   * Fault injector that will always throw an error
   */
  public static class StringFaultInjector implements FaultInjector {
    private final String info;

    public StringFaultInjector(String info) {
      this.info = info;
    }

    @Override
    public Object[] injectFault(StackTraceElement[] trace) {
      if (ErrorHandlingUtils.stackContainsClass(trace, TestFaultInjecting.class)) {
        return new String[] { info };
      }
      return null;
    }
  }
}
