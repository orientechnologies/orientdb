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
package com.orientechnologies.orient.server.network.protocol.http.command.get;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.server.config.OServerEntryConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;

public class OServerCommandGetServer extends OServerCommandGetConnections {
  private static final String[] NAMES = { "GET|server" };

  public OServerCommandGetServer() {
    super("server.info");
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    checkSyntax(iRequest.url, 1, "Syntax error: server");

    iRequest.data.commandInfo = "Server status";

    try {
      final StringWriter jsonBuffer = new StringWriter();
      final OJSONWriter json = new OJSONWriter(jsonBuffer);
      json.beginObject();

      writeConnections(json, null);
      writeDatabases(json);
      writeStorages(json);
      writeProperties(json);
      writeGlobalProperties(json);
      json.endObject();

      iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, jsonBuffer.toString(), null);

    } finally {
    }
    return false;
  }

  private void writeGlobalProperties(OJSONWriter json) throws IOException {
    json.beginCollection(2, true, "globalProperties");

    for (OGlobalConfiguration c : OGlobalConfiguration.values()) {
      json.beginObject(3, true, null);
      json.writeAttribute(4, false, "key", c.getKey());
      json.writeAttribute(4, false, "description", c.getDescription());
      json.writeAttribute(4, false, "value", c.getValue());
      json.writeAttribute(4, false, "defaultValue", c.getDefValue());
      json.writeAttribute(4, false, "canChange", c.isChangeableAtRuntime());
      json.endObject(3, true);
    }

    json.endCollection(2, true);
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }

  protected void writeProperties(final OJSONWriter json) throws IOException {
    json.beginCollection(2, true, "properties");

    OServerEntryConfiguration[] confProperties = server.getConfiguration().properties;
    if (confProperties != null) {
      for (OServerEntryConfiguration entry : confProperties) {
        json.beginObject(3, true, null);
        json.writeAttribute(4, false, "name", entry.name);
        json.writeAttribute(4, false, "value", entry.value);
        json.endObject(3, true);
      }
    }
    json.endCollection(2, true);
  }

  protected void writeStorages(final OJSONWriter json) throws IOException {
    json.beginCollection(1, true, "storages");
    Collection<OStorage> storages = Orient.instance().getStorages();
    for (OStorage s : storages) {
      json.beginObject(2);
      writeField(json, 2, "name", s.getName());
      writeField(json, 2, "type", s.getClass().getSimpleName());
      writeField(json, 2, "path",
          s instanceof OLocalPaginatedStorage ? ((OLocalPaginatedStorage) s).getStoragePath().replace('\\', '/') : "");
      json.endObject(2);
    }
    json.endCollection(1, false);
  }

  protected void writeDatabases(final OJSONWriter json) throws IOException {
    json.beginCollection(1, true, "dbs");
    Collection<OPartitionedDatabasePool> dbPools = server.getDatabasePoolFactory().getPools();
    for (OPartitionedDatabasePool pool : dbPools) {
      writeField(json, 2, "db", pool.getUrl());
      writeField(json, 2, "user", pool.getUserName());
      json.endObject(2);
    }
    json.endCollection(1, false);
  }

  private void writeField(final OJSONWriter json, final int iLevel, final String iAttributeName, final Object iAttributeValue)
      throws IOException {
    json.writeAttribute(iLevel, true, iAttributeName, iAttributeValue != null ? iAttributeValue.toString() : "-");
  }
}
