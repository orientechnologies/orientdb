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
package com.orientechnologies.orient.server;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.disk.OLocalPaginatedStorage;
import com.orientechnologies.orient.server.config.OServerEntryConfiguration;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocolData;
import java.io.IOException;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.List;

/**
 * Returns information about the server.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OServerInfo {
  public static String getServerInfo(final OServer server) throws IOException {
    final StringWriter jsonBuffer = new StringWriter();
    final OJSONWriter json = new OJSONWriter(jsonBuffer);
    json.beginObject();

    getConnections(server, json, null);
    getDatabases(server, json);
    getStorages(server, json);
    getProperties(server, json);
    getGlobalProperties(server, json);

    json.endObject();

    return jsonBuffer.toString();
  }

  public static void getConnections(
      final OServer server, final OJSONWriter json, final String databaseName) throws IOException {
    final DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    json.beginCollection(1, true, "connections");

    final List<OClientConnection> conns = server.getClientConnectionManager().getConnections();
    for (OClientConnection c : conns) {
      final ONetworkProtocolData data = c.getData();
      final OClientConnectionStats stats = c.getStats();

      if (databaseName != null && !databaseName.equals((stats.lastDatabase)))
        // SKIP IT
        continue;

      final String lastCommandOn;
      final String connectedOn;

      synchronized (dateTimeFormat) {
        lastCommandOn = dateTimeFormat.format(new Date(stats.lastCommandReceived));
        connectedOn = dateTimeFormat.format(new Date(c.getSince()));
      }
      String lastDatabase;
      String lastUser;
      if (stats.lastDatabase != null && stats.lastUser != null) {
        lastDatabase = stats.lastDatabase;
        lastUser = stats.lastUser;
      } else {
        lastDatabase = data.lastDatabase;
        lastUser = data.lastUser;
      }
      json.beginObject(2);
      writeField(json, 2, "connectionId", c.getId());
      writeField(
          json,
          2,
          "remoteAddress",
          c.getProtocol().getChannel() != null
              ? c.getProtocol().getChannel().toString()
              : "Disconnected");
      writeField(json, 2, "db", lastDatabase != null ? lastDatabase : "-");
      writeField(json, 2, "user", lastUser != null ? lastUser : "-");
      writeField(json, 2, "totalRequests", stats.totalRequests);
      writeField(json, 2, "commandInfo", data.commandInfo);
      writeField(json, 2, "commandDetail", data.commandDetail);
      writeField(json, 2, "lastCommandOn", lastCommandOn);
      writeField(json, 2, "lastCommandInfo", stats.lastCommandInfo);
      writeField(json, 2, "lastCommandDetail", stats.lastCommandDetail);
      writeField(json, 2, "lastExecutionTime", stats.lastCommandExecutionTime);
      writeField(json, 2, "totalWorkingTime", stats.totalCommandExecutionTime);
      writeField(json, 2, "activeQueries", stats.activeQueries);
      writeField(json, 2, "connectedOn", connectedOn);
      writeField(json, 2, "protocol", c.getProtocol().getType());
      writeField(json, 2, "sessionId", data.sessionId);
      writeField(json, 2, "clientId", data.clientId);

      final StringBuilder driver = new StringBuilder(128);
      if (data.driverName != null) {
        driver.append(data.driverName);
        driver.append(" v");
        driver.append(data.driverVersion);
        driver.append(" Protocol v");
        driver.append(data.protocolVersion);
      }

      writeField(json, 2, "driver", driver.toString());
      json.endObject(2);
    }
    json.endCollection(1, false);
  }

  public static void getGlobalProperties(final OServer server, final OJSONWriter json)
      throws IOException {
    json.beginCollection(2, true, "globalProperties");

    for (OGlobalConfiguration c : OGlobalConfiguration.values()) {
      json.beginObject(3, true, null);
      json.writeAttribute(4, false, "key", c.getKey());
      json.writeAttribute(4, false, "description", c.getDescription());
      json.writeAttribute(4, false, "value", c.isHidden() ? "<hidden>" : c.getValue());
      json.writeAttribute(4, false, "defaultValue", c.getDefValue());
      json.writeAttribute(4, false, "canChange", c.isChangeableAtRuntime());
      json.endObject(3, true);
    }

    json.endCollection(2, true);
  }

  public static void getProperties(final OServer server, final OJSONWriter json)
      throws IOException {
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

  public static void getStorages(final OServer server, final OJSONWriter json) throws IOException {
    json.beginCollection(1, true, "storages");
    Collection<OStorage> storages = server.getDatabases().getStorages();
    for (OStorage s : storages) {
      json.beginObject(2);
      writeField(json, 2, "name", s.getName());
      writeField(json, 2, "type", s.getClass().getSimpleName());
      writeField(
          json,
          2,
          "path",
          s instanceof OLocalPaginatedStorage
              ? ((OLocalPaginatedStorage) s).getStoragePath().toString().replace('\\', '/')
              : "");
      writeField(json, 2, "activeUsers", "n.a.");
      json.endObject(2);
    }
    json.endCollection(1, false);
  }

  public static void getDatabases(final OServer server, final OJSONWriter json) throws IOException {
    json.beginCollection(1, true, "dbs");
    // TODO:get this info from somewhere else
    //    if (!server.getDatabasePoolFactory().isClosed()) {
    //      Collection<OPartitionedDatabasePool> dbPools =
    // server.getDatabasePoolFactory().getPools();
    //      for (OPartitionedDatabasePool pool : dbPools) {
    //        writeField(json, 2, "db", pool.getUrl());
    //        writeField(json, 2, "user", pool.getUserName());
    //        json.endObject(2);
    //      }
    //    }
    json.endCollection(1, false);
  }

  private static void writeField(
      final OJSONWriter json,
      final int iLevel,
      final String iAttributeName,
      final Object iAttributeValue)
      throws IOException {
    json.writeAttribute(
        iLevel, true, iAttributeName, iAttributeValue != null ? iAttributeValue.toString() : "-");
  }
}
