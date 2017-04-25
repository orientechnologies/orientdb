/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OStreamable;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Represents a specific momentum in distributed environment.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class ODistributedMomentum implements OStreamable {
  private static final String LAST_OPERATION_TIME_STAMP = "lastOperationTimeStamp";
  private static final String VERSION                   = "version";
  private final ODocument configuration;

  public ODistributedMomentum() {
    configuration = new ODocument();
  }

  public ODistributedMomentum(final ODocument document) {
    configuration = document;
  }

  public OLogSequenceNumber getLSN(final String iNode) {
    synchronized (configuration) {
      final ODocument embedded = configuration.field(iNode);
      if (embedded == null)
        return null;

      return new OLogSequenceNumber((Long) embedded.field("segment"), (Long) embedded.field("position"));
    }
  }

  public void setLSN(final String iNode, final OLogSequenceNumber iLSN) {
    final ODocument embedded = new ODocument();
    embedded.field("segment", iLSN.getSegment(), OType.LONG);
    embedded.field("position", iLSN.getPosition(), OType.LONG);

    synchronized (configuration) {
      configuration.field(iNode, embedded, OType.EMBEDDED);
      incrementVersion();
    }
  }

  public long getLastOperationTimestamp() {
    synchronized (configuration) {
      final Long ts = configuration.field("lastOperationTimeStamp");
      if (ts == null)
        return -1;

      return ts;
    }
  }

  public void setLastOperationTimestamp(final long lastOperationTimestamp) {
    synchronized (configuration) {
      configuration.field(LAST_OPERATION_TIME_STAMP, lastOperationTimestamp);
      incrementVersion();
    }
  }

  public boolean isEmpty() {
    synchronized (configuration) {
      return configuration.isEmpty();
    }
  }

  public void fromJSON(final InputStream is) throws IOException {
    synchronized (configuration) {
      configuration.fromJSON(is);
    }
  }

  public void toJSON(final OutputStream os) throws IOException {
    synchronized (configuration) {
      configuration.toJSON(os);
    }
  }

  private void incrementVersion() {
    // INCREMENT VERSION
    Integer oldVersion = configuration.field("version");
    if (oldVersion == null)
      oldVersion = 0;
    configuration.field(VERSION, oldVersion.intValue() + 1);
  }

  @Override
  public void toStream(final DataOutput out) throws IOException {
    synchronized (configuration) {
      final byte[] buffer = configuration.toStream();
      out.writeInt(buffer.length);
      out.write(buffer);
    }
  }

  @Override
  public void fromStream(final DataInput in) throws IOException {
    synchronized (configuration) {
      final int length = in.readInt();
      final byte[] buffer = new byte[length];
      in.readFully(buffer);
      configuration.fromStream(buffer);
    }
  }

  public ODistributedMomentum copy() {
    synchronized (configuration) {
      return new ODistributedMomentum(configuration.copy());
    }
  }

  @Override
  public String toString() {
    synchronized (configuration) {
      return configuration.toString();
    }
  }

  public Collection<String> getServers() {
    final List<String> result = new ArrayList<String>();
    synchronized (configuration) {
      for (String s : configuration.fieldNames()) {
        if (!LAST_OPERATION_TIME_STAMP.equals(s) && !VERSION.equals(s))
          result.add(s);
      }
    }
    return result;
  }
}
