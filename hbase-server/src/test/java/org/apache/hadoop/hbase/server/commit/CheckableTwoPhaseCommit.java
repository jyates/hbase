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
package org.apache.hadoop.hbase.server.commit;

import org.apache.hadoop.hbase.server.commit.TwoPhaseCommit;
import org.apache.hadoop.hbase.server.commit.TwoPhaseCommitErrorListener;
import org.apache.hadoop.hbase.server.errorhandling.impl.ExceptionSnare;
import org.mockito.Mockito;

/**
 * An empty two-phase commit operation. Should be used with {@link Mockito#spy(Object)} to check for
 * progress.
 */
public class CheckableTwoPhaseCommit extends
    TwoPhaseCommit<TwoPhaseCommitErrorListener<Exception>, Exception> {
  public boolean prepared = false;
  boolean commit = false;
  boolean cleanup = false;
  boolean finish = false;

  public CheckableTwoPhaseCommit(ExceptionSnare<Exception> monitor,
      TwoPhaseCommitErrorListener<Exception> errorListener, long wakeFrequency) {
    super(monitor, errorListener, wakeFrequency);
  }

  public CheckableTwoPhaseCommit(ExceptionSnare<Exception> monitor,
      TwoPhaseCommitErrorListener<Exception> errorListener, long wakeFrequency,
      int i,
      int j, int k, int l) {
    super(monitor, errorListener, wakeFrequency, i, j, k, l);
  }

  @Override
  public void prepare() throws Exception {
  }

  @Override
  public void commit() throws Exception {
  }

  @Override
  public void cleanup(Exception e) {
  }

  @Override
  public void finish() {
  }
}