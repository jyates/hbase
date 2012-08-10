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
package org.apache.hadoop.hbase.server.commit.distributed;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.protobuf.generated.DistributedCommitProtos.CommitPhase;
import org.apache.hadoop.hbase.protobuf.generated.ErrorHandlingProtos.RemoteFailureException;
import org.apache.hadoop.hbase.server.errorhandling.exception.OperationAttemptTimeoutException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Triple;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test that we correctly serialize exceptions from a remote source
 */
@Category(SmallTests.class)
public class TestRemoteExceptionSerialization {

  private static final String nodeName = "someNode";
  @Test
  public void testTimeoutException() {
    OperationAttemptTimeoutException in = new OperationAttemptTimeoutException(1, 2, 0);
    RemoteFailureException msg = RemoteExceptionSerializer.buildRemoteException(nodeName, in);
    RemoteTaskTimeoutException out = RemoteExceptionSerializer.getTimeoutException(msg);
    assertEquals("Node name got corrupted", nodeName, out.getSourceNode());
    assertEquals("Start time corrupted on transfer", in.getStart(), out.getStart());
    assertEquals("End time corrupted on transfer", in.getEnd(), out.getEnd());
    assertEquals("Allowed time corrupted on transfer", in.getMaxAllowedOperationTime(),
      out.getMaxAllowedOperationTime());
  }

  @Test
  public void testSimpleException() {
    String data = "some bytes";
    byte[] specialData = Bytes.toBytes(data);
    DistributedCommitException in = new DistributedCommitException(specialData);
    // check that we get the data back out
    RemoteFailureException msg = new RemoteExceptionSerializer(nodeName).buildRemoteException(
      CommitPhase.COMMIT, in);
    Triple<CommitPhase, DistributedCommitException, String> triple = RemoteExceptionSerializer
        .unwind(msg);
    assertEquals("Got a different commit phase", CommitPhase.COMMIT, triple.getFirst());
    assertEquals("Original exception bytes were corrupted", data,
      Bytes.toString(triple.getSecond().getExceptionInfo()));
    assertEquals("Node name not preserver", nodeName, triple.getThird());

    // now check that we get the right stack trace
    StackTraceElement elem = new StackTraceElement(this.getClass().toString(), "method", "file", 1);
    in.setStackTrace(new StackTraceElement[] { elem });
    msg = new RemoteExceptionSerializer(nodeName).buildRemoteException(CommitPhase.COMMIT, in);
    triple = RemoteExceptionSerializer.unwind(msg);
    assertEquals("Got a different commit phase", CommitPhase.COMMIT, triple.getFirst());
    assertEquals("Original exception bytes were corrupted", data,
      Bytes.toString(triple.getSecond().getExceptionInfo()));
    assertEquals("Stack trace got corrupted", elem, triple.getSecond().getStackTrace()[0]);
    assertEquals("Got an unexpectedly long stack trace", 1,
      triple.getSecond().getStackTrace().length);
    assertEquals("Node name not preserver", nodeName, triple.getThird());
  }

  @Test
  public void testRemoteFromLocal() {
    String errorMsg = "some message";
    Exception generic = new Exception(errorMsg);
    RemoteFailureException msg = new RemoteExceptionSerializer(nodeName).buildFromUnreadableRemoteException(generic);
    Triple<CommitPhase, DistributedCommitException, String> e = RemoteExceptionSerializer
        .unwind(msg);
    assertEquals("Local failures wrapped as remote exceptions don't have a known phase",
      CommitPhase.UNKNOWN, e.getFirst());
    StackTraceElement[] elems = generic.getStackTrace();
    StackTraceElement[] elems2 = e.getSecond().getStackTrace();
    // elems2 = Arrays.copyOfRange(elems2, 1, elems2.length);
    // assertThat(elems2, new ArrayEquals(generic.getStackTrace()));
    assertArrayEquals("Local stack trace got corrupted", generic.getStackTrace(), elems2);
    assertEquals("Node name not preserver", nodeName, e.getThird());
  }
}