/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.server.network.protocol.http.command.get;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.common.concur.resource.OResourcePool;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;
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

      json.endObject();

      iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, jsonBuffer.toString(), null);

    } finally {
    }
    return false;
  }

  protected void writeProperties(final OJSONWriter json) throws IOException {
    json.beginCollection(2, true, "properties");
    for (OServerEntryConfiguration entry : server.getConfiguration().properties) {
      json.beginObject(3, true, null);
      json.writeAttribute(4, false, "name", entry.name);
      json.writeAttribute(4, false, "value", entry.value);
      json.endObject(3, true);
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
          s instanceof OStorageLocalAbstract ? ((OStorageLocalAbstract) s).getStoragePath().replace('\\', '/') : "");
      writeField(json, 2, "activeUsers", s.getUsers());
      json.endObject(2);
    }
    json.endCollection(1, false);
  }

  protected void writeDatabases(final OJSONWriter json) throws IOException {
    json.beginCollection(1, true, "dbs");
    Map<String, OResourcePool<String, ODatabaseDocumentTx>> dbPool = server.getDatabasePool().getPools();
    for (Entry<String, OResourcePool<String, ODatabaseDocumentTx>> entry : dbPool.entrySet()) {
      for (ODatabaseDocumentTx db : entry.getValue().getResources()) {

        json.beginObject(2);
        writeField(json, 2, "db", db.getName());
        writeField(json, 2, "user", db.getUser() != null ? db.getUser().getName() : "-");
        writeField(json, 2, "status", db.isClosed() ? "closed" : "open");
        writeField(json, 2, "type", db.getType());
        writeField(json, 2, "storageType", db.getStorage().getType());
        json.endObject(2);
      }
    }
    json.endCollection(1, false);
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }

  private void writeField(final OJSONWriter json, final int iLevel, final String iAttributeName, final Object iAttributeValue)
      throws IOException {
    json.writeAttribute(iLevel, true, iAttributeName, iAttributeValue != null ? iAttributeValue.toString() : "-");
  }
}
