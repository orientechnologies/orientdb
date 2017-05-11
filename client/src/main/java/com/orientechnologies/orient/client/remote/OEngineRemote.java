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
package com.orientechnologies.orient.client.remote;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.engine.OEngineAbstract;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.storage.OStorage;

import java.util.Map;

/**
 * Remote engine implementation.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OEngineRemote extends OEngineAbstract {
  public static final String NAME   = "remote";
  public static final String PREFIX = NAME + ":";
  protected volatile ORemoteConnectionManager connectionManager;

  public OEngineRemote() {
  }

  public OStorageRemote createStorage(final String iURL, final Map<String, String> iConfiguration) {
    try {
      return new OStorageRemote(iURL, "rw", getConnectionManager());
    } catch (Exception e) {
      final String message = "Error on opening database: " + iURL;
      OLogManager.instance().error(this, message, e);
      throw OException.wrapException(new ODatabaseException(message), e);
    }
  }

  @Override
  public void removeStorage(final OStorage iStorage) {
  }

  @Override
  public void startup() {
    super.startup();

    connectionManager = new ORemoteConnectionManager(OGlobalConfiguration.NETWORK_LOCK_TIMEOUT.getValueAsLong());
  }

  @Override
  public void shutdown() {
    try {
      connectionManager.close();
    } finally {
      super.shutdown();
    }
  }

  @Override
  public String getNameFromPath(String dbPath) {
    return dbPath;
  }

  public ORemoteConnectionManager getConnectionManager() {
    return connectionManager;
  }

  public String getName() {
    return NAME;
  }

}
