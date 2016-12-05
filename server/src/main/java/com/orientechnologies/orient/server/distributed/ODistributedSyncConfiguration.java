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

import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;

import java.io.*;

/**
 * Immutable class to store and handle information about synchronization between nodes.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ODistributedSyncConfiguration {
  private static final String LAST_OPERATION_TIME_STAMP = "lastOperationTimeStamp";
  private ODocument           configuration;
  private File                file;

  public ODistributedSyncConfiguration(final File file) throws IOException {
    configuration = new ODocument();
    this.file = file;

    if (!file.exists()) {
      file.getParentFile().mkdirs();
      file.createNewFile();
      return;
    }

    final InputStream is = new FileInputStream(file);
    try {
      configuration.fromJSON(is);

    } catch (OSerializationException e) {
      // CORRUPTED: RECREATE IT
      file.getParentFile().mkdirs();
      file.createNewFile();

    } finally {
      is.close();
    }
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

  public void setLastOperationTimestamp(final long lastOperationTimestamp) throws IOException {
    synchronized (configuration) {
      configuration.field(LAST_OPERATION_TIME_STAMP, lastOperationTimestamp);
      incrementVersion();
    }
  }

  public void save() throws IOException {
    if (!file.exists()) {
      file.getParentFile().mkdirs();
      file.createNewFile();
    }

    final OutputStream os = new FileOutputStream(file, false);
    try {

      synchronized (configuration) {
        configuration.toJSON(os);
      }
    } finally {
      os.close();
    }
  }

  public boolean isEmpty() {
    synchronized (configuration) {
      return configuration.isEmpty();
    }
  }

  private void incrementVersion() {
    // INCREMENT VERSION
    Integer oldVersion = configuration.field("version");
    if (oldVersion == null)
      oldVersion = 0;
    configuration.field("version", oldVersion.intValue() + 1);
  }
}
