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
package org.apache.hadoop.hbase.client;

import java.util.UUID;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * An {@link OperationWithAttributes} that is tied to a specific cluster
 */
public abstract class ClusterOperation extends OperationWithAttributes {
  // Attribute used in Mutations to indicate the originating cluster.
  protected static final String CLUSTER_ID_ATTR = "_c.id_";

  /**
   * Set the replication cluster id.
   * @param clusterId
   */
  public void setClusterId(UUID clusterId) {
    byte[] val = new byte[2 * Bytes.SIZEOF_LONG];
    Bytes.putLong(val, 0, clusterId.getMostSignificantBits());
    Bytes.putLong(val, Bytes.SIZEOF_LONG, clusterId.getLeastSignificantBits());
    setAttribute(CLUSTER_ID_ATTR, val);
  }

  /**
   * @return The replication cluster id.
   */
  public UUID getClusterId() {
    byte[] attr = getAttribute(CLUSTER_ID_ATTR);
    if (attr == null) {
      return HConstants.DEFAULT_CLUSTER_ID;
    }
    return new UUID(Bytes.toLong(attr, 0), Bytes.toLong(attr, Bytes.SIZEOF_LONG));
  }
}
