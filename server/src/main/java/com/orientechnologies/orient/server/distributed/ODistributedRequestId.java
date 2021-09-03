/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.serialization.OStreamable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Immutable object representing the distributed request id.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class ODistributedRequestId implements Comparable, OStreamable, Externalizable {

  private int nodeId;
  private long messageId;

  public ODistributedRequestId() {}

  public ODistributedRequestId(final int iNodeId, final long iMessageId) {
    nodeId = iNodeId;
    messageId = iMessageId;
  }

  public long getMessageId() {
    return messageId;
  }

  public int getNodeId() {
    return nodeId;
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof ODistributedRequestId)) return false;

    final ODistributedRequestId other = (ODistributedRequestId) obj;
    return nodeId == other.nodeId && messageId == other.messageId;
  }

  @Override
  public int compareTo(final Object obj) {
    if (!(obj instanceof ODistributedRequestId)) return -1;

    final ODistributedRequestId other = (ODistributedRequestId) obj;
    return Integer.compare(hashCode(), other.hashCode());
  }

  @Override
  public int hashCode() {
    return 31 * nodeId + 103 * (int) messageId;
  }

  public void toStream(final DataOutput out) throws IOException {
    out.writeInt(nodeId);
    out.writeLong(messageId);
  }

  public void fromStream(final DataInput in) throws IOException {
    nodeId = in.readInt();
    messageId = in.readLong();
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeInt(nodeId);
    out.writeLong(messageId);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    nodeId = in.readInt();
    messageId = in.readLong();
  }

  @Override
  public String toString() {
    return nodeId + "." + messageId;
  }
}
