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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Base {@link ErrorMonitor} that actually keeps track of the errors received. It has a couple of
 * nice features:
 * <ol>
 * <li>preventing infinite recursion of error propagation (if it received an error, which in turns
 * causes another process to fail when notified, we don't propagate the error again)</li>
 * <li>also ensures that the error should even be passed onto the listeners via a
 * {@link ReceiveErrorChecker}</li>
 * </ol>
 * @param <T> type of delegate to actually receive the error notification
 * @param <E> exception that should be thrown when there is an error.
 */
public class BaseBoundErrorMonitor<T, E extends Exception> extends BoundErrorDispatcher<T>
    implements ErrorMonitor<E> {
  private static final Log LOG = LogFactory.getLog(BaseBoundErrorMonitor.class);

  private volatile boolean error = false;
  private ReceiveErrorChecker checker;
  protected volatile E exception;
  private ErrorFailureThrower<E> thrower;

  public BaseBoundErrorMonitor(ErrorListenerBridge<T> transformer,
      ReceiveErrorChecker checker, ErrorFailureThrower<E> thrower) {
    super(transformer);
    this.checker = checker;
    this.thrower = thrower;
    // always listen for errors to itself
    this.addErrorListener(this);
  }

  @Override
  public void receiveError(String message, Object... info) {
    LOG.error("Got an error:" + message + ", info:" + info);
    // if we already got the error or we received the error
    if (this.error || !checker.shouldRecieveError(info)) return;

    // convert the info to an exception that we can throw as necessary
    this.exception = thrower.convertToException(message, info);
    this.error = true;
    // make sure to pass on the error to all listeners via the superclass
    super.receiveError(message, info);
  }

  @Override
  public boolean checkForError() {
    return this.error;
  }

  @Override
  public void failOnError() throws E {
    if (this.checkForError()) {
      throw exception;
    }
  }
}