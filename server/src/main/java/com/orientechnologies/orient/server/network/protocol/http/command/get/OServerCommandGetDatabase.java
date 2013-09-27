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
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;

public class OServerCommandGetDatabase extends OServerCommandGetConnect {
  private static final String[] NAMES = { "GET|database/*" };

  @Override
  public void configure(final OServer server) {
    super.configure(server);
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    String[] urlParts = checkSyntax(iRequest.url, 2, "Syntax error: database/<database>");

    iRequest.data.commandInfo = "Database info";
    iRequest.data.commandDetail = urlParts[1];

    exec(iRequest, iResponse, urlParts);
    return false;
  }

  protected void exec(final OHttpRequest iRequest, final OHttpResponse iResponse, final String[] urlParts)
      throws InterruptedException, IOException {
    ODatabaseDocumentTx db = null;
    try {
      if (urlParts.length > 2) {
        server.getDatabasePool().acquire(urlParts[1], urlParts[2], urlParts[3]);
      } else
        db = getProfiledDatabaseInstance(iRequest);

      final StringWriter buffer = new StringWriter();
      final OJSONWriter json = new OJSONWriter(buffer);
      json.beginObject();

      json.beginObject("server");
      json.writeAttribute("version", OConstants.ORIENT_VERSION);
      if (OConstants.getBuildNumber() != null)
        json.writeAttribute("build", OConstants.getBuildNumber());
      json.writeAttribute("osName", System.getProperty("os.name"));
      json.writeAttribute("osVersion", System.getProperty("os.version"));
      json.writeAttribute("osArch", System.getProperty("os.arch"));
      json.writeAttribute("javaVendor", System.getProperty("java.vm.vendor"));
      json.writeAttribute("javaVersion", System.getProperty("java.vm.version"));
      json.endObject();

      if (db.getMetadata().getSchema().getClasses() != null) {
        json.beginCollection("classes");
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
        json.endCollection();
      }

      if (db.getStorage() instanceof OStorageLocal) {
        json.beginCollection("dataSegments");
        for (ODataLocal data : ((OStorageLocal) db.getStorage()).getDataSegments()) {
          if (data != null) {
            json.beginObject();
            json.writeAttribute("id", data.getId());
            json.writeAttribute("name", data.getName());
            json.writeAttribute("size", data.getSize());
            json.writeAttribute("filled", data.getFilledUpTo());
            json.writeAttribute("maxSize", data.getConfig().maxSize);
            json.writeAttribute("files", Arrays.toString(data.getConfig().infoFiles));
            json.endObject();
          }
        }
        json.endCollection();
      }

      if (db.getClusterNames() != null) {
        json.beginCollection("clusters");
        OCluster cluster;
        for (String clusterName : db.getClusterNames()) {
          cluster = db.getStorage().getClusterById(db.getClusterIdByName(clusterName));

          try {
            json.beginObject();
            json.writeAttribute("id", cluster.getId());
            json.writeAttribute("name", clusterName);
            json.writeAttribute("type", cluster.getType());
            json.writeAttribute("records", cluster.getEntries() - cluster.getTombstonesCount());
            if (cluster instanceof OClusterLocal) {
              json.writeAttribute("size", ((OClusterLocal) cluster).getSize());
              json.writeAttribute("filled", ((OClusterLocal) cluster).getFilledUpTo());
              json.writeAttribute("maxSize", ((OClusterLocal) cluster).getConfig().getMaxSize());
              json.writeAttribute("files", Arrays.toString(((OClusterLocal) cluster).getConfig().getInfoFiles()));
            } else {
              json.writeAttribute("size", "-");
              json.writeAttribute("filled", "-");
              json.writeAttribute("maxSize", "-");
              json.writeAttribute("files", "-");
            }
          } catch (Exception e) {
            json.writeAttribute("records", "? (Unauthorized)");
          }
          json.endObject();
        }
        json.endCollection();
      }

      if (db.getStorage() instanceof OStorageLocal) {
        json.beginCollection("txSegment");
        final OTxSegment txSegment = ((OStorageLocal) db.getStorage()).getTxManager().getTxSegment();
        json.beginObject();
        json.writeAttribute("size", txSegment.getSize());
        json.writeAttribute("filled", txSegment.getFilledUpTo());
        json.writeAttribute("maxSize", txSegment.getConfig().maxSize);
        json.writeAttribute("file", txSegment.getConfig().path);
        json.endObject();
        json.endCollection();
      }

      if (db.getUser() != null) {
        json.writeAttribute("currentUser", db.getUser().getName());

        json.beginCollection("users");
        for (ODocument doc : db.getMetadata().getSecurity().getAllUsers()) {
          OUser user = new OUser(doc);
          json.beginObject();
          json.writeAttribute("name", user.getName());
          json.writeAttribute("roles", user.getRoles() != null ? Arrays.toString(user.getRoles().toArray()) : "null");
          json.endObject();
        }
        json.endCollection();

        json.beginCollection("roles");
        ORole role;
        for (ODocument doc : db.getMetadata().getSecurity().getAllRoles()) {
          role = new ORole(doc);
          json.beginObject();
          json.writeAttribute("name", role.getName());
          json.writeAttribute("mode", role.getMode().toString());

          json.beginCollection("rules");
          if (role.getRules() != null) {
            for (Entry<String, Byte> rule : role.getRules().entrySet()) {
              json.beginObject();
              json.writeAttribute("name", rule.getKey());
              json.writeAttribute("create", role.allow(rule.getKey(), ORole.PERMISSION_CREATE));
              json.writeAttribute("read", role.allow(rule.getKey(), ORole.PERMISSION_READ));
              json.writeAttribute("update", role.allow(rule.getKey(), ORole.PERMISSION_UPDATE));
              json.writeAttribute("delete", role.allow(rule.getKey(), ORole.PERMISSION_DELETE));
              json.endObject();
            }
          }
          json.endCollection();

          json.endObject();
        }
        json.endCollection();
      }

      json.beginObject("config");

      json.beginCollection("values");
      json.writeObjects(null, new Object[] { "name", "dateFormat", "value", db.getStorage().getConfiguration().dateFormat },
          new Object[] { "name", "dateTimeFormat", "value", db.getStorage().getConfiguration().dateTimeFormat }, new Object[] {
              "name", "localeCountry", "value", db.getStorage().getConfiguration().getLocaleCountry() }, new Object[] { "name",
              "localeLanguage", "value", db.getStorage().getConfiguration().getLocaleLanguage() }, new Object[] { "name",
              "charSet", "value", db.getStorage().getConfiguration().getCharset() }, new Object[] { "name", "timezone", "value",
              db.getStorage().getConfiguration().getTimeZone().getID() }, new Object[] { "name", "definitionVersion", "value",
              db.getStorage().getConfiguration().version });
      json.endCollection();

      json.beginCollection("properties");
      if (db.getStorage().getConfiguration().properties != null)
        for (OStorageEntryConfiguration entry : db.getStorage().getConfiguration().properties) {
          if (entry != null) {
            json.beginObject();
            json.writeAttribute("name", entry.name);
            json.writeAttribute("value", entry.value);
            json.endObject();
          }
        }
      json.endCollection();

      json.endObject();
      json.endObject();
      json.flush();

      iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, buffer.toString(), null);
    } finally {
      if (db != null)
        db.close();
    }
  }

  public static void exportClass(final ODatabaseDocumentTx db, final OJSONWriter json, final OClass cls) throws IOException {
    json.beginObject();
    json.writeAttribute("name", cls.getName());
    json.writeAttribute("superClass", cls.getSuperClass() != null ? cls.getSuperClass().getName() : "");
    json.writeAttribute("alias", cls.getShortName());
    json.writeAttribute("abstract", cls.isAbstract());
    json.writeAttribute("clusters", cls.getClusterIds());
    json.writeAttribute("defaultCluster", cls.getDefaultClusterId());
    if (cls instanceof OClassImpl) {
      final Map<String, String> custom = ((OClassImpl) cls).getCustomInternal();
      if (custom != null && !custom.isEmpty()) {
        json.writeAttribute("custom", custom);
      }
    }

    try {
      json.writeAttribute("records", db.countClass(cls.getName()));
    } catch (OSecurityAccessException e) {
      json.writeAttribute("records", "? (Unauthorized)");
    }

    if (cls.properties() != null && cls.properties().size() > 0) {
      json.beginCollection("properties");
      for (final OProperty prop : cls.properties()) {
        json.beginObject();
        json.writeAttribute("name", prop.getName());
        if (prop.getLinkedClass() != null)
          json.writeAttribute("linkedClass", prop.getLinkedClass().getName());
        if (prop.getLinkedType() != null)
          json.writeAttribute("linkedType", prop.getLinkedType().toString());
        json.writeAttribute("type", prop.getType().toString());
        json.writeAttribute("mandatory", prop.isMandatory());
        json.writeAttribute("readonly", prop.isReadonly());
        json.writeAttribute("notNull", prop.isNotNull());
        json.writeAttribute("min", prop.getMin());
        json.writeAttribute("max", prop.getMax());

        if (prop instanceof OPropertyImpl) {
          final Map<String, String> custom = ((OPropertyImpl) prop).getCustomInternal();
          if (custom != null && !custom.isEmpty()) {
            json.writeAttribute("custom", custom);
          }
        }

        json.endObject();
      }
      json.endCollection();
    }

    final Set<OIndex<?>> indexes = cls.getIndexes();
    if (!indexes.isEmpty()) {
      json.beginCollection("indexes");
      for (final OIndex<?> index : indexes) {
        json.beginObject();
        json.writeAttribute("name", index.getName());
        json.writeAttribute("type", index.getType());

        final OIndexDefinition indexDefinition = index.getDefinition();
        if (indexDefinition != null && !indexDefinition.getFields().isEmpty())
          json.writeAttribute("fields", indexDefinition.getFields());
        json.endObject();
      }
      json.endCollection();
    }

    json.endObject();
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
