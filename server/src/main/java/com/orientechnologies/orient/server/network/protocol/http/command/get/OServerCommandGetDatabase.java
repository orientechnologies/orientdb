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
package com.orientechnologies.orient.server.network.protocol.http.command.get;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.config.OStorageEntryConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OPropertyImpl;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OServerCommandGetDatabase extends OServerCommandGetConnect {
  private static final String[] NAMES = {"GET|database/*"};

  public static void exportClass(
      final ODatabaseDocument db, final OJSONWriter json, final OClass cls) throws IOException {
    json.beginObject();
    json.writeAttribute("name", cls.getName());
    json.writeAttribute(
        "superClass", cls.getSuperClass() != null ? cls.getSuperClass().getName() : "");

    json.beginCollection("superClasses");
    int i = 0;
    for (OClass oClass : cls.getSuperClasses()) {
      json.write((i > 0 ? "," : "") + "\"" + oClass.getName() + "\"");
      i++;
    }
    json.endCollection();

    json.writeAttribute("alias", cls.getShortName());
    json.writeAttribute("abstract", cls.isAbstract());
    json.writeAttribute("strictmode", cls.isStrictMode());
    json.writeAttribute("clusters", cls.getClusterIds());
    json.writeAttribute("defaultCluster", cls.getDefaultClusterId());
    json.writeAttribute("clusterSelection", cls.getClusterSelection().getName());
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
    } catch (Exception e) {
      json.writeAttribute("records", "? (Error)");
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
        json.writeAttribute("regexp", prop.getRegexp());
        json.writeAttribute(
            "collate", prop.getCollate() != null ? prop.getCollate().getName() : "default");
        json.writeAttribute("defaultValue", prop.getDefaultValue());

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

    final Set<OIndex> indexes = cls.getIndexes();
    if (!indexes.isEmpty()) {
      json.beginCollection("indexes");
      for (final OIndex index : indexes) {
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
  public void configure(final OServer server) {
    super.configure(server);
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    String[] urlParts = checkSyntax(iRequest.getUrl(), 2, "Syntax error: database/<database>");

    iRequest.getData().commandInfo = "Database info";
    iRequest.getData().commandDetail = urlParts[1];

    exec(iRequest, iResponse, urlParts);
    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }

  protected void exec(
      final OHttpRequest iRequest, final OHttpResponse iResponse, final String[] urlParts)
      throws InterruptedException, IOException {
    ODatabaseDocumentInternal db = null;
    try {
      if (urlParts.length > 2) {
        db = server.openDatabase(urlParts[1], urlParts[2], urlParts[3]);
      } else db = getProfiledDatabaseInstance(iRequest);

      final StringWriter buffer = new StringWriter();
      final OJSONWriter json = new OJSONWriter(buffer);
      json.beginObject();

      json.beginObject("server");
      json.writeAttribute("version", OConstants.getRawVersion());
      if (OConstants.getBuildNumber() != null)
        json.writeAttribute("build", OConstants.getBuildNumber());
      json.writeAttribute("osName", System.getProperty("os.name"));
      json.writeAttribute("osVersion", System.getProperty("os.version"));
      json.writeAttribute("osArch", System.getProperty("os.arch"));
      json.writeAttribute("javaVendor", System.getProperty("java.vm.vendor"));
      json.writeAttribute("javaVersion", System.getProperty("java.vm.version"));

      json.beginCollection("conflictStrategies");

      Set<String> strategies =
          Orient.instance().getRecordConflictStrategy().getRegisteredImplementationNames();

      int i = 0;
      for (String strategy : strategies) {
        json.write((i > 0 ? "," : "") + "\"" + strategy + "\"");
        i++;
      }
      json.endCollection();

      json.beginCollection("clusterSelectionStrategies");
      Set<String> clusterSelectionStrategies =
          db.getMetadata()
              .getImmutableSchemaSnapshot()
              .getClusterSelectionFactory()
              .getRegisteredNames();
      int j = 0;
      for (String strategy : clusterSelectionStrategies) {
        json.write((j > 0 ? "," : "") + "\"" + strategy + "\"");
        j++;
      }
      json.endCollection();

      json.endObject();

      if (((OMetadataInternal) db.getMetadata()).getImmutableSchemaSnapshot().getClasses()
          != null) {
        json.beginCollection("classes");
        List<String> classNames = new ArrayList<String>();

        for (OClass cls : db.getMetadata().getSchema().getClasses()) classNames.add(cls.getName());
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

      if (db.getClusterNames() != null) {
        json.beginCollection("clusters");
        for (String clusterName : db.getClusterNames()) {
          final int clusterId = db.getClusterIdByName(clusterName);
          if (clusterId < 0) {
            continue;
          }
          try {
            final String conflictStrategy = db.getClusterRecordConflictStrategy(clusterId);

            json.beginObject();
            json.writeAttribute("id", clusterId);
            json.writeAttribute("name", clusterName);
            json.writeAttribute("records", db.countClusterElements(clusterId));
            json.writeAttribute("conflictStrategy", conflictStrategy);
            json.writeAttribute("size", "-");
            json.writeAttribute("filled", "-");
            json.writeAttribute("maxSize", "-");
            json.writeAttribute("files", "-");
          } catch (Exception e) {
            json.writeAttribute("records", "? (Unauthorized)");
          }
          json.endObject();
        }
        json.endCollection();
      }

      if (db.getUser() != null) {
        json.writeAttribute("currentUser", db.getUser().getName());

        // exportSecurityInfo(db, json);
      }
      final OIndexManagerAbstract idxManager = db.getMetadata().getIndexManagerInternal();
      json.beginCollection("indexes");
      for (OIndex index : idxManager.getIndexes(db)) {
        json.beginObject();
        try {
          json.writeAttribute("name", index.getName());
          json.writeAttribute("configuration", index.getConfiguration());
          // Exclude index size because it's too costly
          // json.writeAttribute("size", index.getSize());
        } catch (Exception e) {
          OLogManager.instance().error(this, "Cannot serialize index configuration", e);
        }
        json.endObject();
      }
      json.endCollection();

      json.beginObject("config");

      json.beginCollection("values");
      OStorageConfiguration configuration = db.getStorageInfo().getConfiguration();
      json.writeObjects(
          null,
          new Object[] {"name", "dateFormat", "value", configuration.getDateFormat()},
          new Object[] {"name", "dateTimeFormat", "value", configuration.getDateTimeFormat()},
          new Object[] {"name", "localeCountry", "value", configuration.getLocaleCountry()},
          new Object[] {"name", "localeLanguage", "value", configuration.getLocaleLanguage()},
          new Object[] {"name", "charSet", "value", configuration.getCharset()},
          new Object[] {"name", "timezone", "value", configuration.getTimeZone().getID()},
          new Object[] {"name", "definitionVersion", "value", configuration.getVersion()},
          new Object[] {"name", "clusterSelection", "value", configuration.getClusterSelection()},
          new Object[] {"name", "minimumClusters", "value", configuration.getMinimumClusters()},
          new Object[] {"name", "conflictStrategy", "value", configuration.getConflictStrategy()});
      json.endCollection();

      json.beginCollection("properties");
      if (configuration.getProperties() != null)
        for (OStorageEntryConfiguration entry : configuration.getProperties()) {
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

      iResponse.send(
          OHttpUtils.STATUS_OK_CODE,
          OHttpUtils.STATUS_OK_DESCRIPTION,
          OHttpUtils.CONTENT_JSON,
          buffer.toString(),
          null);
    } finally {
      if (db != null) db.close();
    }
  }

  private void exportSecurityInfo(ODatabaseDocument db, OJSONWriter json) throws IOException {
    json.beginCollection("users");
    for (ODocument doc : db.getMetadata().getSecurity().getAllUsers()) {
      OUser user = new OUser(doc);
      json.beginObject();
      json.writeAttribute("name", user.getName());
      json.writeAttribute(
          "roles", user.getRoles() != null ? Arrays.toString(user.getRoles().toArray()) : "null");
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
        for (Map.Entry<String, Byte> rule : role.getRules().entrySet()) {
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
}
