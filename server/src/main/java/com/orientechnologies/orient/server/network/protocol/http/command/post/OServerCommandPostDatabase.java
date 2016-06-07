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
package com.orientechnologies.orient.server.network.protocol.http.command.post;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OStorageEntryConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.engine.local.OEngineLocalPaginated;
import com.orientechnologies.orient.core.engine.memory.OEngineMemory;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedServerAbstract;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class OServerCommandPostDatabase extends OServerCommandAuthenticatedServerAbstract {
  private static final String[] NAMES = { "POST|database/*" };

  public OServerCommandPostDatabase() {
    super("database.create");
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    String[] urlParts = checkSyntax(iRequest.url, 3, "Syntax error: database/<db>/<type>");

    iRequest.data.commandInfo = "Create database";

    try {
      final String databaseName = urlParts[1];
      final String storageMode = urlParts[2];
      String url = getStoragePath(databaseName, storageMode);
      final String type = urlParts.length > 3 ? urlParts[3] : "document";
      if (url != null) {
        final ODatabaseDocumentInternal database = new ODatabaseDocumentTx(url);
        if (database.exists()) {
          iResponse.send(OHttpUtils.STATUS_CONFLICT_CODE, OHttpUtils.STATUS_CONFLICT_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN,
              "Database '" + database.getURL() + "' already exists.", null);
        } else {
          for (OStorage stg : Orient.instance().getStorages()) {
            if (stg.getName().equalsIgnoreCase(database.getName()) && stg.exists())
              throw new ODatabaseException("Database named '" + database.getName() + "' already exists: " + stg);
          }
          OLogManager.instance().info(this, "Creating database: " + url);
          database.create();
          sendDatabaseInfo(iRequest, iResponse, database);
        }
      } else {
        throw new OCommandExecutionException("The '" + storageMode + "' storage mode does not exists.");
      }
    } finally {
    }
    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }

  protected String getStoragePath(final String databaseName, final String iStorageMode) {
    if (iStorageMode.equals(OEngineLocalPaginated.NAME))
      return iStorageMode + ":" + server.getDatabaseDirectory() + databaseName;
    else if (iStorageMode.equals(OEngineMemory.NAME))
      return iStorageMode + ":" + databaseName;

    return null;
  }

  protected void sendDatabaseInfo(final OHttpRequest iRequest, final OHttpResponse iResponse, final ODatabaseDocumentInternal db)
      throws IOException {
    final StringWriter buffer = new StringWriter();
    final OJSONWriter json = new OJSONWriter(buffer);

    json.beginObject();
    
    if (db.getMetadata().getSchema().getClasses() != null) {
      json.beginCollection(1, false, "classes");
      Set<String> exportedNames = new HashSet<String>();
      for (OClass cls : db.getMetadata().getSchema().getClasses()) {
        if (!exportedNames.contains(cls.getName()))
          try {
            exportClass(db, json, cls);
            exportedNames.add(cls.getName());
          } catch (Exception e) {
            OLogManager.instance().error(this, "Error on exporting class '" + cls + "'", e);
          }
      }
      json.endCollection(1, true);
    }

    if (db.getClusterNames() != null) {
      json.beginCollection(1, false, "clusters");
      OCluster cluster;
      for (String clusterName : db.getClusterNames()) {
        cluster = db.getStorage().getClusterById(db.getClusterIdByName(clusterName));

        try {
          json.beginObject(2, true, null);
          json.writeAttribute(3, false, "id", cluster.getId());
          json.writeAttribute(3, false, "name", clusterName);
          json.writeAttribute(3, false, "records", cluster.getEntries() - cluster.getTombstonesCount());
          json.writeAttribute(3, false, "size", "-");
          json.writeAttribute(3, false, "filled", "-");
          json.writeAttribute(3, false, "maxSize", "-");
          json.writeAttribute(3, false, "files", "-");
        } catch (Exception e) {
          json.writeAttribute(3, false, "records", "? (Unauthorized)");
        }
        json.endObject(2, false);
      }
      json.endCollection(1, true);
    }

    if(db.getUser() != null)
    	json.writeAttribute(1, false, "currentUser", db.getUser().getName());

    json.beginCollection(1, false, "users");
    OUser user;
    for (ODocument doc : db.getMetadata().getSecurity().getAllUsers()) {    	
      user = new OUser(doc);
      json.beginObject(2, true, null);
      json.writeAttribute(3, false, "name", user.getName());
      json.writeAttribute(3, false, "roles", user.getRoles() != null ? Arrays.toString(user.getRoles().toArray()) : "null");
      json.endObject(2, false);
    }
    json.endCollection(1, true);

    json.beginCollection(1, true, "roles");
    ORole role;
    for (ODocument doc : db.getMetadata().getSecurity().getAllRoles()) {
      role = new ORole(doc);
      json.beginObject(2, true, null);
      json.writeAttribute(3, false, "name", role.getName());
      json.writeAttribute(3, false, "mode", role.getMode().toString());

      json.beginCollection(3, true, "rules");
      for (Map.Entry<String, Byte> rule : role.getRules().entrySet()) {
        json.beginObject(4);
        json.writeAttribute(4, true, "name", rule.getKey());
        json.writeAttribute(4, false, "create", role.allow(rule.getKey(), ORole.PERMISSION_CREATE));
        json.writeAttribute(4, false, "read", role.allow(rule.getKey(), ORole.PERMISSION_READ));
        json.writeAttribute(4, false, "update", role.allow(rule.getKey(), ORole.PERMISSION_UPDATE));
        json.writeAttribute(4, false, "delete", role.allow(rule.getKey(), ORole.PERMISSION_DELETE));
        json.endObject(4, true);
      }
      json.endCollection(3, false);

      json.endObject(2, true);
    }
    json.endCollection(1, true);

    json.beginObject(1, true, "config");

    json.beginCollection(2, true, "values");
    json.writeObjects(3, true, null, new Object[] { "name", "dateFormat", "value", db.getStorage().getConfiguration().dateFormat },
        new Object[] { "name", "dateTimeFormat", "value", db.getStorage().getConfiguration().dateTimeFormat }, new Object[] {
            "name", "localeCountry", "value", db.getStorage().getConfiguration().getLocaleCountry() }, new Object[] { "name",
            "localeLanguage", "value", db.getStorage().getConfiguration().getLocaleLanguage() }, new Object[] { "name",
            "definitionVersion", "value", db.getStorage().getConfiguration().version });
    json.endCollection(2, true);

    json.beginCollection(2, true, "properties");
    if (db.getStorage().getConfiguration().getProperties() != null)
      for (OStorageEntryConfiguration entry : db.getStorage().getConfiguration().getProperties()) {
        if (entry != null) {
          json.beginObject(3, true, null);
          json.writeAttribute(4, false, "name", entry.name);
          json.writeAttribute(4, false, "value", entry.value);
          json.endObject(3, true);
        }
      }
    json.endCollection(2, true);

    json.endObject(1, true);
    json.endObject();
    json.flush();

    iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, buffer.toString(), null);
  }

  protected void exportClass(final ODatabaseDocument db, final OJSONWriter json, final OClass cls) throws IOException {
    json.beginObject(2, true, null);
    json.writeAttribute(3, true, "name", cls.getName());
    json.writeAttribute(3, true, "superClass", cls.getSuperClass() != null ? cls.getSuperClass().getName() : "");
    json.writeAttribute(3, true, "alias", cls.getShortName());
    json.writeAttribute(3, true, "clusters", cls.getClusterIds());
    json.writeAttribute(3, true, "defaultCluster", cls.getDefaultClusterId());
    json.writeAttribute(3, true, "clusterSelection", cls.getClusterSelection().getName());
    try {
      json.writeAttribute(3, false, "records", db.countClass(cls.getName()));
    } catch (OSecurityAccessException e) {
      json.writeAttribute(3, false, "records", "? (Unauthorized)");
    }

    if (cls.properties() != null && cls.properties().size() > 0) {
      json.beginCollection(3, true, "properties");
      for (final OProperty prop : cls.properties()) {
        json.beginObject(4, true, null);
        json.writeAttribute(4, true, "name", prop.getName());
        if (prop.getLinkedClass() != null)
          json.writeAttribute(4, true, "linkedClass", prop.getLinkedClass().getName());
        if (prop.getLinkedType() != null)
          json.writeAttribute(4, true, "linkedType", prop.getLinkedType().toString());
        json.writeAttribute(4, true, "type", prop.getType().toString());
        json.writeAttribute(4, true, "mandatory", prop.isMandatory());
        json.writeAttribute(4, true, "readonly", prop.isReadonly());
        json.writeAttribute(4, true, "notNull", prop.isNotNull());
        json.writeAttribute(4, true, "min", prop.getMin());
        json.writeAttribute(4, true, "max", prop.getMax());
        json.endObject(3, true);
      }
      json.endCollection(1, true);
    }

    final Set<OIndex<?>> indexes = cls.getIndexes();
    if (!indexes.isEmpty()) {
      json.beginCollection(3, true, "indexes");
      for (final OIndex<?> index : indexes) {
        json.beginObject(4, true, null);
        json.writeAttribute(4, true, "name", index.getName());
        json.writeAttribute(4, true, "type", index.getType());

        final OIndexDefinition indexDefinition = index.getDefinition();
        if (indexDefinition != null && !indexDefinition.getFields().isEmpty())
          json.writeAttribute(4, true, "fields", indexDefinition.getFields());
        json.endObject(3, true);
      }
      json.endCollection(1, true);
    }

    json.endObject(1, false);
  }

}
