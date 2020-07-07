/*
 *
 *  *  Copyright 2016 Orient Technologies LTD (info(at)orientdb.com)
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
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Immutable class to store and handle information about synchronization between nodes.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class ODistributedSyncConfiguration {
  private final Map<String, OLogSequenceNumber> lastLSN =
      new ConcurrentHashMap<String, OLogSequenceNumber>();
  private final ODistributedMomentum momentum;
  private File file;
  private long lastOperationTimestamp = -1;
  private long lastLSNWrittenOnDisk = 0l;

  public ODistributedSyncConfiguration(final File file) throws IOException {
    momentum = new ODistributedMomentum();
    this.file = file;

    if (!file.exists()) {
      file.getParentFile().mkdirs();
      file.createNewFile();
      return;
    }
    load();
  }

  public ODistributedMomentum getMomentum() {
    updateInternalDocument();
    return momentum;
  }

  public void load() throws IOException {
    final InputStream is = new FileInputStream(file);
    try {
      synchronized (momentum) {
        momentum.fromJSON(is);

        lastOperationTimestamp = momentum.getLastOperationTimestamp();
        lastLSN.clear();
        for (String server : momentum.getServers()) {
          lastLSN.put(server, momentum.getLSN(server));
        }
      }

    } catch (OSerializationException e) {
      // CORRUPTED: RECREATE IT
      file.getParentFile().mkdirs();
      file.createNewFile();

    } finally {
      is.close();
    }
  }

  public void save() throws IOException {
    updateInternalDocument();

    lastLSNWrittenOnDisk = System.currentTimeMillis();

    if (!file.exists()) {
      file.getParentFile().mkdirs();
      file.createNewFile();
    }

    final OutputStream os = new FileOutputStream(file, false);
    try {
      momentum.toJSON(os);
    } finally {
      os.close();
    }
  }

  public long getLastOperationTimestamp() {
    return lastOperationTimestamp;
  }

  protected void updateInternalDocument() {
    momentum.setLastOperationTimestamp(lastOperationTimestamp);
    for (Map.Entry<String, OLogSequenceNumber> entry : lastLSN.entrySet())
      momentum.setLSN(entry.getKey(), entry.getValue());
  }

  public void removeServer(final String nodeName) throws IOException {
    if (lastLSN.remove(nodeName) != null) save();
  }
}
