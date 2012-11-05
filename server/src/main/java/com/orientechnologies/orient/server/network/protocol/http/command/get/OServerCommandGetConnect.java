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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.config.OStorageEntryConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OPropertyImpl;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.impl.local.OClusterLocal;
import com.orientechnologies.orient.core.storage.impl.local.ODataLocal;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;
import com.orientechnologies.orient.core.storage.impl.local.OTxSegment;
import com.orientechnologies.orient.server.db.OSharedDocumentDatabase;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

public class OServerCommandGetConnect extends OServerCommandAuthenticatedDbAbstract {
  private static final String[] NAMES = { "GET|connect/*" };

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    final String[] urlParts = checkSyntax(iRequest.url, 2, "Syntax error: connect/<database>[/<user>/<password>]");

    urlParts[1] = urlParts[1].replace(DBNAME_DIR_SEPARATOR, '/');

    iRequest.data.commandInfo = "Connect";
    iRequest.data.commandDetail = urlParts[1];

    exec(iRequest, iResponse, urlParts);
    return false;
  }

  @Override
  public boolean beforeExecute(OHttpRequest iRequest, OHttpResponse iResponse) throws IOException {
    final String[] urlParts = checkSyntax(iRequest.url, 2, "Syntax error: connect/<database>[/<user>/<password>]");

    if (urlParts == null || urlParts.length < 3)
      return super.beforeExecute(iRequest, iResponse);

    // USER+PASSWD AS PARAMETERS
    return true;
  }

  protected void exec(final OHttpRequest iRequest, final OHttpResponse iResponse, String[] urlParts) throws InterruptedException,
      IOException {
    ODatabaseDocumentTx db = null;
    try {
      if (urlParts.length > 2) {
        db = OSharedDocumentDatabase.acquire(urlParts[1], urlParts[2], urlParts[3]);
      } else
        db = getProfiledDatabaseInstance(iRequest);

      final StringWriter buffer = new StringWriter();
      final OJSONWriter json = new OJSONWriter(buffer);
      json.beginObject();

      json.beginObject(1, true, "server");
      json.writeAttribute(2, true, "version", OConstants.ORIENT_VERSION);
      if (OConstants.getBuildNumber() != null)
        json.writeAttribute(2, true, "build", OConstants.getBuildNumber());
      json.writeAttribute(2, true, "osName", System.getProperty("os.name"));
      json.writeAttribute(2, true, "osVersion", System.getProperty("os.version"));
      json.writeAttribute(2, true, "osArch", System.getProperty("os.arch"));
      json.writeAttribute(2, true, "javaVendor", System.getProperty("java.vm.vendor"));
      json.writeAttribute(2, true, "javaVersion", System.getProperty("java.vm.version"));
      json.endObject(1, true);

      if (db.getMetadata().getSchema().getClasses() != null) {
        json.beginCollection(1, true, "classes");
        List<String> classNames = new ArrayList<String>();

        for (OClass cls : db.getMetadata().getSchema().getClasses())
          classNames.add(cls.getName());
        Collections.sort(classNames);

        for (String className : classNames) {
          final OClass cls = db.getMetadata().getSchema().getClass(className);

          try {
            exportClass(db, json, cls);
          } catch (Exception e) {
            OLogManager.instance().error(this, "Error on exporting class '" + cls + "'", e);
          }
        }
        json.endCollection(1, true);
      }

      if (db.getStorage() instanceof OStorageLocal) {
        json.beginCollection(1, false, "dataSegments");
        for (ODataLocal data : ((OStorageLocal) db.getStorage()).getDataSegments()) {
          json.beginObject(2, true, null);
          json.writeAttribute(3, false, "id", data.getId());
          json.writeAttribute(3, false, "name", data.getName());
          json.writeAttribute(3, false, "size", data.getSize());
          json.writeAttribute(3, false, "filled", data.getFilledUpTo());
          json.writeAttribute(3, false, "maxSize", data.getConfig().maxSize);
          json.writeAttribute(3, false, "files", Arrays.toString(data.getConfig().infoFiles));
          json.endObject(2, false);
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
            json.writeAttribute(3, false, "type", cluster.getType());
            json.writeAttribute(3, false, "records", cluster.getEntries());
            if (cluster instanceof OClusterLocal) {
              json.writeAttribute(3, false, "size", ((OClusterLocal) cluster).getSize());
              json.writeAttribute(3, false, "filled", ((OClusterLocal) cluster).getFilledUpTo());
              json.writeAttribute(3, false, "maxSize", ((OClusterLocal) cluster).getConfig().getMaxSize());
              json.writeAttribute(3, false, "files", Arrays.toString(((OClusterLocal) cluster).getConfig().getInfoFiles()));
            } else {
              json.writeAttribute(3, false, "size", "-");
              json.writeAttribute(3, false, "filled", "-");
              json.writeAttribute(3, false, "maxSize", "-");
              json.writeAttribute(3, false, "files", "-");
            }
          } catch (Exception e) {
            json.writeAttribute(3, false, "records", "? (Unauthorized)");
          }
          json.endObject(2, false);
        }
        json.endCollection(1, true);
      }

      if (db.getStorage() instanceof OStorageLocal) {
        json.beginCollection(1, false, "txSegment");
        final OTxSegment txSegment = ((OStorageLocal) db.getStorage()).getTxManager().getTxSegment();
        json.beginObject(2, true, null);
        json.writeAttribute(3, false, "size", txSegment.getSize());
        json.writeAttribute(3, false, "filled", txSegment.getFilledUpTo());
        json.writeAttribute(3, false, "maxSize", txSegment.getConfig().maxSize);
        json.writeAttribute(3, false, "file", txSegment.getConfig().path);
        json.endObject(2, false);
        json.endCollection(1, true);
      }

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
        for (Entry<String, Byte> rule : role.getRules().entrySet()) {
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
      json.writeObjects(3, true, null,
          new Object[] { "name", "dateFormat", "value", db.getStorage().getConfiguration().dateFormat }, new Object[] { "name",
              "dateTimeFormat", "value", db.getStorage().getConfiguration().dateTimeFormat }, new Object[] { "name",
              "localeCountry", "value", db.getStorage().getConfiguration().getLocaleCountry() }, new Object[] { "name",
              "localeLanguage", "value", db.getStorage().getConfiguration().getLocaleLanguage() }, new Object[] { "name",
              "definitionVersion", "value", db.getStorage().getConfiguration().version });
      json.endCollection(2, true);

      json.beginCollection(2, true, "properties");
      if (db.getStorage().getConfiguration().properties != null)
        for (OStorageEntryConfiguration entry : db.getStorage().getConfiguration().properties) {
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
    } finally {
      if (db != null)
        OSharedDocumentDatabase.release(db);
    }
  }

  public static void exportClass(final ODatabaseDocumentTx db, final OJSONWriter json, final OClass cls) throws IOException {
    json.beginObject(2, true, null);
    json.writeAttribute(3, true, "name", cls.getName());
    json.writeAttribute(3, true, "superClass", cls.getSuperClass() != null ? cls.getSuperClass().getName() : "");
    json.writeAttribute(3, true, "alias", cls.getShortName());
    json.writeAttribute(3, true, "abstract", cls.isAbstract());
    json.writeAttribute(3, true, "clusters", cls.getClusterIds());
    json.writeAttribute(3, true, "defaultCluster", cls.getDefaultClusterId());
    if (cls instanceof OClassImpl) {
      final Map<String, String> custom = ((OClassImpl) cls).getCustomInternal();
      if (custom != null && !custom.isEmpty()) {
        json.writeAttribute(4, true, "custom", custom);
      }
    }

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
        json.writeAttribute(4, true, "notNull", prop.isNotNull());
        json.writeAttribute(4, true, "min", prop.getMin());
        json.writeAttribute(4, true, "max", prop.getMax());

        if (prop instanceof OPropertyImpl) {
          final Map<String, String> custom = ((OPropertyImpl) prop).getCustomInternal();
          if (custom != null && !custom.isEmpty()) {
            json.writeAttribute(5, true, "custom", custom);
          }
        }

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

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
