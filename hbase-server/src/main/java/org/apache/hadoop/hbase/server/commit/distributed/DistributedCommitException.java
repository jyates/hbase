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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.server.commit.ThreePhaseCommit;

/**
 * Exception from running a distributed {@link ThreePhaseCommit} to allow passing of exception
 * data/information between members of the commit cohort and the commit coordinator.
 * <p>
 * Data passed should be serialized via protocol buffers to ensure operation progression in the face
 * of partial failures due to server upgrades.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
@SuppressWarnings("serial")
public class DistributedCommitException extends Exception {

  private final byte[] data;

  public DistributedCommitException(byte[] data) {
    this("", data);
  }

  public DistributedCommitException(Exception cause) {
    super(cause);
    data = null;
  }

  public DistributedCommitException(String msg) {
    super(msg);
    data = null;
  }

  public DistributedCommitException(String message, Exception cause) {
    super(message, cause);
    data = null;
  }

  public DistributedCommitException(Exception cause, byte[] data) {
    super(cause);
    this.data = data;
  }

  public DistributedCommitException(String msg, byte[] data) {
    super(msg);
    this.data = data;
  }

  public DistributedCommitException(String message, Exception cause, byte[] data) {
    super(message, cause);
    this.data = data;
  }

  public byte[] getExceptionInfo() {
    return this.data;
  }
}