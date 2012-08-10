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
import org.apache.hadoop.hbase.server.errorhandling.exception.OperationAttemptTimeoutException;

/**
 * A {@link OperationAttemptTimeoutException} that caused a task failure on a remote node.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
@SuppressWarnings("serial")
public class RemoteTaskTimeoutException extends OperationAttemptTimeoutException {

  private final String nodeName;

  /**
   * @param nodeName name of the node that originally threw the exception
   * @param start time the timer started
   * @param end time the timer ended
   * @param allowed max allowed runtime
   */
  public RemoteTaskTimeoutException(String nodeName, long start, long end, long allowed) {
    super(start, end, allowed);
    this.nodeName = nodeName;
  }

  public String getSourceNode() {
    return nodeName;
  }

  public String toString() {
    return "Node:" + nodeName + " timed out!\n" + super.toString();
  }
}
