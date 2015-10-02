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
package com.orientechnologies.orient.client.remote;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.engine.OEngineAbstract;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.sql.parser.OWait;
import com.orientechnologies.orient.core.storage.OStorage;

public class OEngineRemote extends OEngineAbstract {
  public static final String                         NAME           = "remote";
  protected static final Map<String, OStorageRemote> sharedStorages = new HashMap<String, OStorageRemote>();
  protected final ORemoteConnectionManager           connectionManager;

  public OEngineRemote() {
    connectionManager = new ORemoteConnectionManager(OGlobalConfiguration.CLIENT_CHANNEL_MAX_POOL.getValueAsInteger(),
        OGlobalConfiguration.NETWORK_LOCK_TIMEOUT.getValueAsLong());
  }

  public OStorage createStorage(final String iURL, final Map<String, String> iConfiguration) {
    try {
      synchronized (sharedStorages) {
        OStorageRemote sharedStorage = sharedStorages.get(iURL);
        if (sharedStorage == null) {
          sharedStorage = new OStorageRemote(null, iURL, "rw");
          sharedStorages.put(iURL, sharedStorage);
        }

        return new OStorageRemoteThread(sharedStorage);
      }
    } catch (Exception e) {
      final String message = "Error on opening database: " + iURL;
      OLogManager.instance().error(this, message, e);
      throw OException.wrapException(new ODatabaseException(message), e);
    }
  }

  public void removeStorage(final String iURL) {
    synchronized (sharedStorages) {
      sharedStorages.remove(iURL);
    }
  }

  @Override
  public void removeStorage(final OStorage iStorage) {
    synchronized (sharedStorages) {
      for (Entry<String, OStorageRemote> entry : sharedStorages.entrySet()) {
        if (entry.getValue() == iStorage) {
          sharedStorages.remove(entry.getKey());
          break;
        }
      }
    }
  }

  @Override
  public void shutdown() {
    super.shutdown();
    connectionManager.close();
    synchronized (sharedStorages) {
      sharedStorages.clear();
    }
  }

  public ORemoteConnectionManager getConnectionManager() {
    return connectionManager;
  }

  public String getName() {
    return NAME;
  }

  public boolean isShared() {
    return false;
  }
}
