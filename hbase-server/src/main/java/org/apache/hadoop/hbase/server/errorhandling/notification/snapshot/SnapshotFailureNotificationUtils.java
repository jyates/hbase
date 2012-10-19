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
package org.apache.hadoop.hbase.server.errorhandling.notification.snapshot;

import java.util.concurrent.atomic.AtomicLong;

import javax.management.Notification;

import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;

import com.google.common.base.Preconditions;

/**
 * 
 */
public class SnapshotFailureNotificationUtils {

  private static final AtomicLong sequenceNumber = new AtomicLong(0);

  public enum SnapshotFailureTypes {
    LOCAL("local_snasphot"), GLOBAL("global_snapshot");
    private String type;

    private SnapshotFailureTypes(String type) {
      this.type = type;
    }

    public String getType() {
      return this.type;
    }

  }

  public static Notification mapLocalSnapshotFailure(Object caller, HBaseSnapshotException e) {
    Preconditions
        .checkNotNull(
          e.getSnapshotDescription(),
          "Must have a SnapshotDescription to send a local snapshot failure notification. Maybe you meant to use globalSnapshotFailure?");
    Notification n = new Notification(SnapshotFailureTypes.LOCAL.type, caller,
        sequenceNumber.getAndIncrement());
    n.setUserData(e);
    return n;
  }

  public static Notification mapGlobalSnapshotFailure(Object caller, Throwable e) {
    Notification n = new Notification(SnapshotFailureTypes.GLOBAL.type, caller,
        sequenceNumber.getAndIncrement());
    n.setUserData(e);
    return n;
  }
}