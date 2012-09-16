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
package org.apache.hadoop.hbase.server.snapshot.errorhandling;

import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionVisitor;
import org.apache.hadoop.hbase.server.snapshot.error.SnapshotFailureListener;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;

public class SnapshotExceptionVisitor implements ExceptionVisitor<SnapshotFailureListener> {

  @Override
  public void visit(SnapshotFailureListener listener, String message, Exception e, Object... info) {
    SnapshotDescription snapshot = getSnapshotDescriptionFromErrorInfo(info);
    // if we got a snapshot, then use it
    if (snapshot != null) {
      e = getExceptionAsSnapshotException(message, snapshot, e);
      listener.snapshotFailure(message, snapshot, e);
      return;
    }

    // otherwise, we are expecting a string snapshot name or null
    listener.snapshotFailure(message, getSnapshotNameFromErrorInfo(info));
  }

  protected String getSnapshotNameFromErrorInfo(Object... info) {
    if (info.length == 0) return null;
    if (info[0] instanceof String) return (String) info[0];
    return null;
  }

  protected SnapshotDescription getSnapshotDescriptionFromErrorInfo(Object... info) {
    if (info.length >= 1 && info[0] instanceof SnapshotDescription) {
      return (SnapshotDescription) info[0];
    }
    return null;
  }

  public static HBaseSnapshotException getExceptionAsSnapshotException(String message,
      SnapshotDescription snapshot, Exception e) {
    if (e == null) {
      return new HBaseSnapshotException(message, snapshot);
    }
    if (!(e instanceof HBaseSnapshotException)) {
      return new HBaseSnapshotException(message, e, snapshot);
    } else return (HBaseSnapshotException) e;
  }

  /**
   * An error visitor that only accepts snapshot errors for:
   * <ol>
   * <li>snapshots matching the passed snapshot in either the exception or explicitly in info</li>
   * <li>errors without a snapshot ('generic' errors)</li>
   * </ol>
   */
  public static class BoundSnapshotErrorVisitor extends SnapshotExceptionVisitor {
    private SnapshotDescription snapshot;

    public BoundSnapshotErrorVisitor(SnapshotDescription snapshot) {
      this.snapshot = snapshot;
    }

    @Override
    public void visit(SnapshotFailureListener listener, String message, Exception e, Object... info) {
      // if it is a generic snapshot error, then propagate
      if (info.length == 0) super.visit(listener, message, e, info);
      // otherwise we need to check the info to see if it matches

      SnapshotDescription snapshot = getSnapshotDescriptionFromErrorInfo(info);
      // it wasn't a description, but it might be a name
      if (snapshot == null) {
        String name = getSnapshotNameFromErrorInfo(info);
        if (name == null || this.snapshot.getName().equals(name)) {
          super.visit(listener, message, e, info);
        }
      }
      // it is a snapshot, so check full equality
      if (this.snapshot.equals(snapshot)) {
        super.visit(listener, message, e, info);
      }

    }
  }
}