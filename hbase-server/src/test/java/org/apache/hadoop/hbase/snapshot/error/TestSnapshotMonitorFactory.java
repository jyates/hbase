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
package org.apache.hadoop.hbase.snapshot.error;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription.Type;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionListener;
import org.apache.hadoop.hbase.server.errorhandling.OperationAttemptTimer;
import org.apache.hadoop.hbase.server.errorhandling.impl.ExceptionOrchestrator;
import org.apache.hadoop.hbase.server.snapshot.error.SnapshotErrorListener;
import org.apache.hadoop.hbase.server.snapshot.error.SnapshotErrorMonitorFactory;
import org.apache.hadoop.hbase.server.snapshot.errorhandling.SnapshotExceptionDispatcher;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.exception.SnapshotCreationException;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * Test basic error distribution with a {@link SnapshotErrorMonitorFactory}
 */
@Category(SmallTests.class)
@SuppressWarnings({ "unchecked", "rawtypes" })
public class TestSnapshotMonitorFactory {

  final long waitTime = 10;
  final Configuration conf = new Configuration(false);
  {
    conf.setLong(SnapshotDescriptionUtils.MASTER_WAIT_TIME_GLOBAL_SNAPSHOT, waitTime);
    conf.setLong(SnapshotDescriptionUtils.MASTER_WAIT_TIME_TIMESTAMP_SNAPSHOT, waitTime);
    conf.setLong(SnapshotDescriptionUtils.REGION_WAIT_TIME_GLOBAL_SNAPSHOT, waitTime);
    conf.setLong(SnapshotDescriptionUtils.REGION_WAIT_TIME_TIMESTAMP_SNAPSHOT, waitTime);
  }
  final SnapshotDescription.Builder GLOBAL_BUILDER = SnapshotDescription.newBuilder()
      .setType(Type.GLOBAL).setName("snapshot");
  final SnapshotDescription.Builder TIMESTAMP_BUILDER = SnapshotDescription.newBuilder().setName(
    "snapshot");

  /**
   * Test that the dispatcher handles both too and from failure messages with monitors produced by
   * the factory.
   * @throws Exception on failure
   */
  @Test
  public void testDispatchWithFactory() throws Exception {
    SnapshotErrorMonitorFactory factory = new SnapshotErrorMonitorFactory();
    ExceptionOrchestrator<HBaseSnapshotException> dispatcher = factory
        .getHub();
    ExceptionListener listener1 = Mockito.mock(ExceptionListener.class);
    ExceptionListener listener2 = Mockito.mock(ExceptionListener.class);
    dispatcher.addErrorListener(dispatcher.genericVisitor, listener1);
    dispatcher.addErrorListener(dispatcher.genericVisitor, listener2);

    // also get a listener from the factory
    SnapshotErrorListener monitor = factory.createMonitorForSnapshot(SnapshotDescription
        .getDefaultInstance());
    SnapshotDescription snapshot = SnapshotDescription.getDefaultInstance();
    monitor.snapshotFailure("testing error", snapshot);
    assertTrue(monitor.checkForError());
    Mockito.verify(listener1, Mockito.times(1)).receiveError(Mockito.eq("testing error"),
      Mockito.any(SnapshotCreationException.class),
 Mockito.anyVararg());
    Mockito.verify(listener2, Mockito.times(1)).receiveError(Mockito.eq("testing error"),
      Mockito.any(SnapshotCreationException.class), Mockito.anyVararg());
  }

  /**
   * Test that the process timer causes the snapshot to fail across all error listeners
   * @throws Exception on failure
   */
  @Test
  public void testReceiveSingleSnapshotFailure() throws Exception {
    SnapshotErrorMonitorFactory factory = new SnapshotErrorMonitorFactory();
    ExceptionOrchestrator<HBaseSnapshotException> dispatcher = factory.getHub();
    SnapshotExceptionDispatcher receiver = factory.createMonitorForSnapshot(GLOBAL_BUILDER.build());
    ExceptionListener listener = Mockito.mock(ExceptionListener.class);
    receiver.addErrorListener(dispatcher.genericVisitor, listener);

    // another dispatcher that definitely shouldn't get the exception
    SnapshotExceptionDispatcher otherSnapshotReceiver = factory
        .createMonitorForSnapshot(TIMESTAMP_BUILDER.build());
    ExceptionListener listener2 = Mockito.mock(ExceptionListener.class);
    otherSnapshotReceiver.addErrorListener(dispatcher.genericVisitor, listener);

    final Object[] errorInfo = new Object[] { GLOBAL_BUILDER.build() };
    OperationAttemptTimer timer = new OperationAttemptTimer(dispatcher, 1000000, errorInfo);
    timer.trigger();
    // now pass in another error, but don't have that error propagate
    ExceptionListener listener3 = Mockito.mock(ExceptionListener.class);
    receiver.addErrorListener(dispatcher.genericVisitor, listener3);
    dispatcher.receiveError("some message", new HBaseSnapshotException("error"),
      TIMESTAMP_BUILDER.build());

    // verify all the interactions expected
    Mockito.verifyZeroInteractions(listener2, listener3);
    Mockito.verify(listener, Mockito.times(1)).receiveError(Mockito.anyString(),
      Mockito.any(HBaseSnapshotException.class), Mockito.eq(errorInfo[0]));
  }

  /**
   * Test that an error propagates across the 'hub' stored in the error monitor factory. Any error
   * receiver listening for that snapshot should also find a failure.
   */
  @Test
  public void errorPropagatesAcrossHub() {
    SnapshotErrorMonitorFactory factory = new SnapshotErrorMonitorFactory();
    SnapshotErrorListener receiver = factory.createMonitorForSnapshot(GLOBAL_BUILDER.build());
    SnapshotErrorListener receiver2 = factory.createMonitorForSnapshot(GLOBAL_BUILDER.build());
    receiver.snapshotFailure("test error", GLOBAL_BUILDER.build());
    assertTrue("Initial error didn't register with receiver", receiver.checkForError());
    assertTrue("Error didn't propagate across hub to extened receiver.", receiver2.checkForError());
  }

  /**
   * Test that an error listener for another snapshot doesn't accept an error for another snapshot
   * @throws Exception on failure
   */
  @Test
  public void ignoreErrorsForOtherSnapshots() throws Exception {
    SnapshotErrorMonitorFactory factory = new SnapshotErrorMonitorFactory();
    ExceptionOrchestrator<HBaseSnapshotException> dispatcher = factory
        .getHub();
    SnapshotErrorListener receiver = factory.createMonitorForSnapshot(GLOBAL_BUILDER.build());
    receiver = factory.createMonitorForSnapshot(GLOBAL_BUILDER.build());
    // test a snapshot did doesn't monitor
    dispatcher.receiveError("Test failure", new HBaseSnapshotException("Test"),
      TIMESTAMP_BUILDER.build());
    assertFalse("Snapshot error monitor found error for a snapshot it wasn't monitoring!",
      receiver.checkForError());

    // test a snapshot with a different name, but the same time
    String name = TIMESTAMP_BUILDER.getName();
    dispatcher.receiveError("name_bound_error", null, name + "NOT");
    assertFalse("Snapshot error monitor found error for a snapshot name it wasn't monitoring!",
      receiver.checkForError());

    // then get an error for the snapshot we expect
    dispatcher.receiveError("Test failure", new HBaseSnapshotException("Test"),
      GLOBAL_BUILDER.build());
    assertTrue(
      "Snapshot error monitor didn't register an error for its own snapshot after ignoring other errors!",
      receiver.checkForError());

    // and now try checking for just name bound errors
    receiver = factory.createMonitorForSnapshot(GLOBAL_BUILDER.build());
    dispatcher.receiveError("name_bound_error", null, name);
    assertTrue(
      "Snapshot error monitor didn't register an error for its own snapshot name after ignoring other errors!",
      receiver.checkForError());
  }

  /**
   * Test that snapshot error monitors correctly receive 'generic' error notifications - error
   * notifications that don't have a snapshot.
   * @throws Exception on failure
   */
  @Test
  public void receiveGenericErrors() throws Exception {
    SnapshotErrorMonitorFactory factory = new SnapshotErrorMonitorFactory();
    ExceptionOrchestrator<HBaseSnapshotException> dispatcher = factory
        .getHub();
    // fail with an exception
    SnapshotErrorListener receiver = factory.createMonitorForSnapshot(GLOBAL_BUILDER.build());
    dispatcher.receiveError("Test failure", new HBaseSnapshotException("Test"));
    assertTrue("Snapshot error monitor didn't find the generic errror!", receiver.checkForError());

    // fail with a null exception
    receiver = factory.createMonitorForSnapshot(GLOBAL_BUILDER.build());
    assertFalse("Snapshot error monitor found an error before getting one!",
      receiver.checkForError());
    dispatcher.receiveError("Test failure", null);
    assertTrue("Snapshot error monitor didn't find the generic errror!", receiver.checkForError());
  }
}
