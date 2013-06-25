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
package com.orientechnologies.orient.core.db.tool;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.ODatabase.STATUS;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OClassTrigger;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexManagerProxy;
import com.orientechnologies.orient.core.index.ORuntimeKeyIndexDefinition;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.schema.OPropertyImpl;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OSecurityShared;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OJSONReader;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerJSON;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.type.tree.provider.OMVRBTreeRIDProvider;

/**
 * Import data from a file into a database.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ODatabaseImport extends ODatabaseImpExpAbstract {
  private Map<OPropertyImpl, String> linkedClasses       = new HashMap<OPropertyImpl, String>();
  private Map<OClass, String>        superClasses        = new HashMap<OClass, String>();
  private OJSONReader                jsonReader;
  private ORecordInternal<?>         record;
  private List<String>               recordToDelete      = new ArrayList<String>();
  private boolean                    schemaImported      = false;
  private int                        exporterVersion     = -1;
  private boolean                    hashClustersAreUsed = false;
  private ORID                       schemaRecordId;
  private ORID                       indexMgrRecordId;

  public ODatabaseImport(final ODatabaseDocument database, final String iFileName, final OCommandOutputListener iListener)
      throws IOException {
    super(database, iFileName, iListener);

    InputStream inStream;
    final BufferedInputStream bf = new BufferedInputStream(new FileInputStream(fileName));
    bf.mark(1024);
    try {
      inStream = new GZIPInputStream(bf);
    } catch (Exception e) {
      bf.reset();
      inStream = bf;
    }

    jsonReader = new OJSONReader(new InputStreamReader(inStream));
    database.declareIntent(new OIntentMassiveInsert());
  }

  public ODatabaseImport(final ODatabaseDocument database, final InputStream iStream, final OCommandOutputListener iListener)
      throws IOException {
    super(database, "streaming", iListener);
    jsonReader = new OJSONReader(new InputStreamReader(iStream));
    database.declareIntent(new OIntentMassiveInsert());
  }

  @Override
  public ODatabaseImport setOptions(String iOptions) {
    super.setOptions(iOptions);

    return this;
  }

  public ODatabaseImport importDatabase() {
    try {
      listener.onMessage("\nStarted import of database '" + database.getURL() + "' from " + fileName + "...");

      long time = System.currentTimeMillis();

      jsonReader.readNext(OJSONReader.BEGIN_OBJECT);

      database.getLevel1Cache().setEnable(false);
      database.getLevel2Cache().setEnable(false);
      database.setMVCC(false);
      database.setValidationEnabled(false);

      hashClustersAreUsed = database.getStorage().isHashClustersAreUsed();

      database.setStatus(STATUS.IMPORTING);

      String tag;
      while (jsonReader.hasNext() && jsonReader.lastChar() != '}') {
        tag = jsonReader.readString(OJSONReader.FIELD_ASSIGNMENT);

        if (tag.equals("info"))
          importInfo();
        else if (tag.equals("clusters"))
          importClusters();
        else if (tag.equals("schema"))
          importSchema();
        else if (tag.equals("records"))
          importRecords();
        else if (tag.equals("indexes"))
          importIndexes();
        else if (tag.equals("manualIndexes"))
          importManualIndexes();
      }

      deleteHoleRecords();

      database.setStatus(STATUS.OPEN);

      listener.onMessage("\n\nDatabase import completed in " + ((System.currentTimeMillis() - time)) + " ms");

    } catch (Exception e) {
      System.err.println("Error on database import happened just before line " + jsonReader.getLineNumber() + ", column "
          + jsonReader.getColumnNumber());
      e.printStackTrace();
      throw new ODatabaseExportException("Error on importing database '" + database.getName() + "' from file: " + fileName, e);
    } finally {
      close();
    }

    return this;
  }

  /**
   * Delete all the temporary records created to fill the holes and to mantain the same record ID
   */
  private void deleteHoleRecords() {
    listener.onMessage("\nDelete temporary records...");

    final ORecordId rid = new ORecordId();
    final ODocument doc = new ODocument(rid);
    for (String recId : recordToDelete) {
      doc.reset();
      rid.fromString(recId);
      doc.delete();
    }
    listener.onMessage("OK (" + recordToDelete.size() + " records)");
  }

  private void importInfo() throws IOException, ParseException {
    listener.onMessage("\nImporting database info...");

    jsonReader.readNext(OJSONReader.BEGIN_OBJECT);
    while (jsonReader.lastChar() != '}') {
      final String fieldName = jsonReader.readString(OJSONReader.FIELD_ASSIGNMENT);
      if (fieldName.equals("exporter-version"))
        exporterVersion = jsonReader.readInteger(OJSONReader.NEXT_IN_OBJECT);
      else if (fieldName.equals("schemaRecordId"))
        schemaRecordId = new ORecordId(jsonReader.readString(OJSONReader.NEXT_IN_OBJECT));
      else if (fieldName.equals("indexMgrRecordId"))
        indexMgrRecordId = new ORecordId(jsonReader.readString(OJSONReader.NEXT_IN_OBJECT));
      else
        jsonReader.readNext(OJSONReader.NEXT_IN_OBJECT);
    }
    jsonReader.readNext(OJSONReader.COMMA_SEPARATOR);

    if (schemaRecordId == null)
      schemaRecordId = new ORecordId(database.getStorage().getConfiguration().schemaRecordId);

    if (indexMgrRecordId == null)
      indexMgrRecordId = new ORecordId(database.getStorage().getConfiguration().indexMgrRecordId);

    listener.onMessage("OK");
  }

  private void importManualIndexes() throws IOException, ParseException {
    listener.onMessage("\nImporting manual index entries...");

    ODocument doc = new ODocument();

    // FORCE RELOADING
    database.getMetadata().getIndexManager().reload();
    int n = 0;

    do {
      jsonReader.readNext(OJSONReader.BEGIN_OBJECT);

      jsonReader.readString(OJSONReader.FIELD_ASSIGNMENT);
      final String indexName = jsonReader.readString(OJSONReader.NEXT_IN_ARRAY);

      if (indexName == null || indexName.length() == 0)
        return;

      listener.onMessage("\n- Index '" + indexName + "'...");

      final OIndex<?> index = database.getMetadata().getIndexManager().getIndex(indexName);

      long tot = 0;

      jsonReader.readNext(OJSONReader.BEGIN_COLLECTION);

      do {
        final String value = jsonReader.readString(OJSONReader.NEXT_IN_ARRAY).trim();

        if (!value.isEmpty()) {
          doc = (ODocument) ORecordSerializerJSON.INSTANCE.fromString(value, doc, null);

          if (!doc.<Boolean> field("binary"))
            index.put(doc.field("key"), doc.<OIdentifiable> field("rid"));
          else {
            ORuntimeKeyIndexDefinition<?> runtimeKeyIndexDefinition = (ORuntimeKeyIndexDefinition<?>) index.getDefinition();
            OBinarySerializer<?> binarySerializer = runtimeKeyIndexDefinition.getSerializer();
            index.put(binarySerializer.deserialize(doc.<byte[]> field("key"), 0), doc.<OIdentifiable> field("rid"));
          }
          tot++;
        }
      } while (jsonReader.lastChar() == ',');

      if (index != null) {
        listener.onMessage("OK (" + tot + " entries)");
        n++;
      } else
        listener.onMessage("ERR, the index wasn't found in configuration");

      jsonReader.readNext(OJSONReader.END_OBJECT);
      jsonReader.readNext(OJSONReader.NEXT_IN_ARRAY);

    } while (jsonReader.lastChar() == ',');

    listener.onMessage("\nDone. Imported " + n + " indexes.");

    jsonReader.readNext(OJSONReader.NEXT_IN_OBJECT);
  }

  private void importSchema() throws IOException, ParseException {
    listener.onMessage("\nImporting database schema...");

    jsonReader.readNext(OJSONReader.BEGIN_OBJECT);
    @SuppressWarnings("unused")
    int schemaVersion = jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT).checkContent("\"version\"")
        .readNumber(OJSONReader.ANY_NUMBER, true);
    jsonReader.readNext(OJSONReader.COMMA_SEPARATOR).readNext(OJSONReader.FIELD_ASSIGNMENT).checkContent("\"classes\"")
        .readNext(OJSONReader.BEGIN_COLLECTION);

    long classImported = 0;

    try {
      do {
        jsonReader.readNext(OJSONReader.BEGIN_OBJECT);

        final String className = jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT).checkContent("\"name\"")
            .readString(OJSONReader.COMMA_SEPARATOR);

        String next = jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT).getValue();

        if (next.equals("\"id\"")) {
          // @COMPATIBILITY 1.0rc4 IGNORE THE ID
          next = jsonReader.readString(OJSONReader.COMMA_SEPARATOR);
          next = jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT).getValue();
        }

        final int classDefClusterId;
        if (jsonReader.isContent("\"default-cluster-id\"")) {
          next = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);
          classDefClusterId = Integer.parseInt(next);
        } else
          classDefClusterId = database.getDefaultClusterId();

        String classClusterIds = jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT).checkContent("\"cluster-ids\"")
            .readString(OJSONReader.END_COLLECTION, true).trim();

        jsonReader.readNext(OJSONReader.NEXT_IN_OBJECT);

        OClassImpl cls = (OClassImpl) database.getMetadata().getSchema().getClass(className);

        if (cls != null) {
          if (cls.getDefaultClusterId() != classDefClusterId)
            cls.setDefaultClusterId(classDefClusterId);
        } else
          cls = (OClassImpl) database.getMetadata().getSchema().createClass(className, classDefClusterId);

        if (classClusterIds != null) {
          // REMOVE BRACES
          classClusterIds = classClusterIds.substring(1, classClusterIds.length() - 1);

          // ASSIGN OTHER CLUSTER IDS
          for (int i : OStringSerializerHelper.splitIntArray(classClusterIds)) {
            if (i != -1)
              cls.addClusterId(i);
          }
        }

        String value;
        while (jsonReader.lastChar() == ',') {
          jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT);
          value = jsonReader.getValue();

          if (value.equals("\"strictMode\"")) {
            cls.setStrictMode(jsonReader.readBoolean(OJSONReader.NEXT_IN_OBJECT));
          } else if (value.equals("\"abstract\"")) {
            cls.setAbstract(jsonReader.readBoolean(OJSONReader.NEXT_IN_OBJECT));
          } else if (value.equals("\"oversize\"")) {
            final String oversize = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);
            cls.setOverSize(Float.parseFloat(oversize));
          } else if (value.equals("\"short-name\"")) {
            final String shortName = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);
            cls.setShortName(shortName);
          } else if (value.equals("\"super-class\"")) {
            final String classSuper = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);
            superClasses.put(cls, classSuper);
          } else if (value.equals("\"properties\"")) {
            // GET PROPERTIES
            jsonReader.readNext(OJSONReader.BEGIN_COLLECTION);

            while (jsonReader.lastChar() != ']') {
              importProperty(cls);

              if (jsonReader.lastChar() == '}')
                jsonReader.readNext(OJSONReader.NEXT_IN_ARRAY);
            }
            jsonReader.readNext(OJSONReader.END_OBJECT);
          }
        }

        classImported++;

        jsonReader.readNext(OJSONReader.NEXT_IN_ARRAY);
      } while (jsonReader.lastChar() == ',');

      // REBUILD ALL THE INHERITANCE
      for (Map.Entry<OClass, String> entry : superClasses.entrySet())
        entry.getKey().setSuperClass(database.getMetadata().getSchema().getClass(entry.getValue()));

      // SET ALL THE LINKED CLASSES
      for (Map.Entry<OPropertyImpl, String> entry : linkedClasses.entrySet()) {
        entry.getKey().setLinkedClass(database.getMetadata().getSchema().getClass(entry.getValue()));
      }

      database.getMetadata().getSchema().save();

      listener.onMessage("OK (" + classImported + " classes)");
      schemaImported = true;
      jsonReader.readNext(OJSONReader.END_OBJECT);
      jsonReader.readNext(OJSONReader.COMMA_SEPARATOR);
    } catch (Exception e) {
      e.printStackTrace();
      listener.onMessage("ERROR (" + classImported + " entries): " + e);
    }
  }

  private void importProperty(final OClass iClass) throws IOException, ParseException {
    jsonReader.readNext(OJSONReader.NEXT_OBJ_IN_ARRAY);

    if (jsonReader.lastChar() == ']')
      return;

    final String propName = jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT).checkContent("\"name\"")
        .readString(OJSONReader.COMMA_SEPARATOR);

    String next = jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT).getValue();

    if (next.equals("\"id\"")) {
      // @COMPATIBILITY 1.0rc4 IGNORE THE ID
      next = jsonReader.readString(OJSONReader.COMMA_SEPARATOR);
      next = jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT).getValue();
    }

    next = jsonReader.checkContent("\"type\"").readString(OJSONReader.NEXT_IN_OBJECT);

    final OType type = OType.valueOf(next);

    String attrib;
    String value = null;

    String min = null;
    String max = null;
    String linkedClass = null;
    OType linkedType = null;
    boolean mandatory = false;
    boolean readonly = false;
    boolean notNull = false;
    Map<String, String> customFields = null;

    while (jsonReader.lastChar() == ',') {
      jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT);

      attrib = jsonReader.getValue();
      if (!attrib.equals("\"customFields\""))
        value = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);

      if (attrib.equals("\"min\""))
        min = value;
      else if (attrib.equals("\"max\""))
        max = value;
      else if (attrib.equals("\"linked-class\""))
        linkedClass = value;
      else if (attrib.equals("\"mandatory\""))
        mandatory = Boolean.parseBoolean(value);
      else if (attrib.equals("\"readonly\""))
        readonly = Boolean.parseBoolean(value);
      else if (attrib.equals("\"not-null\""))
        notNull = Boolean.parseBoolean(value);
      else if (attrib.equals("\"linked-type\""))
        linkedType = OType.valueOf(value);
      else if (attrib.equals("\"customFields\""))
        customFields = importCustomFields();
    }

    OPropertyImpl prop = (OPropertyImpl) iClass.getProperty(propName);
    if (prop == null)
      // CREATE IT
      prop = (OPropertyImpl) iClass.createProperty(propName, type);

    prop.setMandatory(mandatory);
    prop.setReadonly(readonly);
    prop.setNotNull(notNull);

    if (min != null)
      prop.setMin(min);
    if (max != null)
      prop.setMax(max);
    if (linkedClass != null)
      linkedClasses.put(prop, linkedClass);
    if (linkedType != null)
      prop.setLinkedType(linkedType);

    if (customFields != null) {
      for (Map.Entry<String, String> entry : customFields.entrySet()) {
        prop.setCustom(entry.getKey(), entry.getValue());
      }
    }
  }

  private Map<String, String> importCustomFields() throws ParseException, IOException {
    Map<String, String> result = new HashMap<String, String>();

    jsonReader.readNext(OJSONReader.BEGIN_OBJECT);

    while (jsonReader.lastChar() != '}') {
      final String key = jsonReader.readString(OJSONReader.FIELD_ASSIGNMENT);
      final String value = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);

      result.put(key, value);
    }

    jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);

    return result;
  }

  private long importClusters() throws ParseException, IOException {
    listener.onMessage("\nImporting clusters...");

    long total = 0;

    jsonReader.readNext(OJSONReader.BEGIN_COLLECTION);

    boolean recreateManualIndex = false;
    if (exporterVersion <= 4) {
      removeDefaultClusters();
      recreateManualIndex = true;
    }

    @SuppressWarnings("unused")
    ORecordId rid = null;
    while (jsonReader.lastChar() != ']') {
      jsonReader.readNext(OJSONReader.BEGIN_OBJECT);

      String name = jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT).checkContent("\"name\"")
          .readString(OJSONReader.COMMA_SEPARATOR);

      if (name.length() == 0)
        name = null;

      if (name != null)
        // CHECK IF THE CLUSTER IS INCLUDED
        if (includeClusters != null) {
          if (!includeClusters.contains(name)) {
            jsonReader.readNext(OJSONReader.NEXT_IN_ARRAY);
            continue;
          }
        } else if (excludeClusters != null) {
          if (excludeClusters.contains(name)) {
            jsonReader.readNext(OJSONReader.NEXT_IN_ARRAY);
            continue;
          }
        }

      int id = jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT).checkContent("\"id\"").readInteger(OJSONReader.COMMA_SEPARATOR);
      String type = jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT).checkContent("\"type\"")
          .readString(OJSONReader.NEXT_IN_OBJECT);

      if (jsonReader.lastChar() == ',') {
        rid = new ORecordId(jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT).checkContent("\"rid\"")
            .readString(OJSONReader.NEXT_IN_OBJECT));
      } else
        rid = null;

      listener.onMessage("\n- Creating cluster " + (name != null ? "'" + name + "'" : "NULL") + "...");

      int clusterId = name != null ? database.getClusterIdByName(name) : -1;
      if (clusterId == -1) {
        // CREATE IT
        clusterId = database.addCluster(type, name, null, null);
      }

      if (clusterId != id) {
        if (database.countClusterElements(clusterId - 1) == 0) {
          listener.onMessage("Found previous version: migrating old clusters...");
          database.dropCluster(name, true);
          clusterId = database.addCluster(type, "temp_" + clusterId, null, null);
          clusterId = database.addCluster(type, name, null, null);
          // recreateManualIndex = true;
        } else
          throw new OConfigurationException("Imported cluster '" + name + "' has id=" + clusterId
              + " different from the original: " + id + ". To continue the import drop the cluster '"
              + database.getClusterNameById(clusterId - 1) + "' that has " + database.countClusterElements(clusterId - 1)
              + " records");
      }

      if (name != null
          && !(name.equalsIgnoreCase(OMetadata.CLUSTER_MANUAL_INDEX_NAME) || name.equalsIgnoreCase(OMetadata.CLUSTER_INTERNAL_NAME) || name
              .equalsIgnoreCase(OMetadata.CLUSTER_INDEX_NAME)))
        database.getStorage().getClusterById(clusterId).truncate();

      listener.onMessage("OK, assigned id=" + clusterId);

      total++;

      jsonReader.readNext(OJSONReader.NEXT_IN_ARRAY);
    }
    jsonReader.readNext(OJSONReader.COMMA_SEPARATOR);

    if (recreateManualIndex) {
      database.addCluster(OStorage.CLUSTER_TYPE.PHYSICAL.toString(), OMetadata.CLUSTER_MANUAL_INDEX_NAME, null, null);
      database.getMetadata().getIndexManager().create();

      listener.onMessage("\nManual index cluster was recreated.");
    }

    listener.onMessage("\nDone. Imported " + total + " clusters");

    return total;
  }

  protected void removeDefaultClusters() {
    listener.onMessage("\nWARN: Exported database does not support manual index separation."
        + " Manual index cluster will be dropped.");

    // In v4 new cluster for manual indexes has been implemented. To keep database consistent we should shift back
    // all clusters and recreate cluster for manual indexes in the end.
    database.dropCluster(OMetadata.CLUSTER_MANUAL_INDEX_NAME, true);

    final OSchema schema = database.getMetadata().getSchema();
    if (schema.existsClass(OUser.CLASS_NAME))
      schema.dropClass(OUser.CLASS_NAME);
    if (schema.existsClass(ORole.CLASS_NAME))
      schema.dropClass(ORole.CLASS_NAME);
    if (schema.existsClass(OSecurityShared.RESTRICTED_CLASSNAME))
      schema.dropClass(OSecurityShared.RESTRICTED_CLASSNAME);
    if (schema.existsClass(OFunction.CLASS_NAME))
      schema.dropClass(OFunction.CLASS_NAME);
    if (schema.existsClass(OMVRBTreeRIDProvider.PERSISTENT_CLASS_NAME))
      schema.dropClass(OMVRBTreeRIDProvider.PERSISTENT_CLASS_NAME);
    if (schema.existsClass(OClassTrigger.CLASSNAME))
      schema.dropClass(OClassTrigger.CLASSNAME);
    schema.save();

    database.dropCluster(OStorage.CLUSTER_DEFAULT_NAME, true);

    database.getStorage().setDefaultClusterId(
        database.addCluster(OStorage.CLUSTER_TYPE.PHYSICAL.toString(), OStorage.CLUSTER_DEFAULT_NAME, null, null));

    // Starting from v4 schema has been moved to internal cluster.
    // Create a stub at #2:0 to prevent cluster position shifting.
    new ODocument().save(OStorage.CLUSTER_DEFAULT_NAME);

    database.getMetadata().getSecurity().create();
  }

  private long importRecords() throws Exception {
    long total = 0;

    jsonReader.readNext(OJSONReader.BEGIN_COLLECTION);

    long totalRecords = 0;

    System.out.print("\nImporting records...");

    ORID rid;
    int lastClusterId = -1;
    long clusterRecords = 0;
    while (jsonReader.lastChar() != ']') {
      rid = importRecord();

      if (rid != null) {
        ++clusterRecords;

        if (lastClusterId == -1)
          lastClusterId = rid.getClusterId();
        else if (rid.getClusterId() != lastClusterId || jsonReader.lastChar() == ']') {
          // CHANGED CLUSTERID: DUMP STATISTICS
          System.out.print("\n- Imported records into cluster '" + database.getClusterNameById(lastClusterId) + "' (id="
              + lastClusterId + "): " + clusterRecords + " records");
          clusterRecords = 0;
          lastClusterId = rid.getClusterId();
        }

        ++totalRecords;
      }
      record = null;
    }

    listener.onMessage("\n\nDone. Imported " + totalRecords + " records\n");

    jsonReader.readNext(OJSONReader.COMMA_SEPARATOR);

    return total;
  }

  private ORID importRecord() throws Exception {
    String value = jsonReader.readString(OJSONReader.END_OBJECT, true);

    // JUMP EMPTY RECORDS
    while (!value.isEmpty() && value.charAt(0) != '{') {
      value = value.substring(1);
    }

    record = null;
    try {
      record = ORecordSerializerJSON.INSTANCE.fromString(value, record, null);

      if (schemaImported && record.getIdentity().equals(schemaRecordId)) {
        // JUMP THE SCHEMA
        return null;
      }

      // CHECK IF THE CLUSTER IS INCLUDED
      if (includeClusters != null) {
        if (!includeClusters.contains(database.getClusterNameById(record.getIdentity().getClusterId()))) {
          jsonReader.readNext(OJSONReader.NEXT_IN_ARRAY);
          return null;
        }
      } else if (excludeClusters != null) {
        if (excludeClusters.contains(database.getClusterNameById(record.getIdentity().getClusterId())))
          return null;
      }

      if (record.getIdentity().getClusterId() == 0 && record.getIdentity().getClusterPosition().longValue() == 1)
        // JUMP INTERNAL RECORDS
        return null;

      if (exporterVersion >= 3) {
        int oridsId = database.getClusterIdByName(OMVRBTreeRIDProvider.PERSISTENT_CLASS_NAME);
        int indexId = database.getClusterIdByName(OMetadata.CLUSTER_INDEX_NAME);

        if (record.getIdentity().getClusterId() == indexId || record.getIdentity().getClusterId() == oridsId)
          // JUMP INDEX RECORDS
          return null;
      }

      final int manualIndexCluster = database.getClusterIdByName(OMetadata.CLUSTER_MANUAL_INDEX_NAME);
      final int internalCluster = database.getClusterIdByName(OMetadata.CLUSTER_INTERNAL_NAME);
      final int indexCluster = database.getClusterIdByName(OMetadata.CLUSTER_INDEX_NAME);

      if (exporterVersion >= 4) {
        if (record.getIdentity().getClusterId() == manualIndexCluster)
          // JUMP INDEX RECORDS
          return null;
      }

      final String rid = record.getIdentity().toString();
      final int clusterId = record.getIdentity().getClusterId();

      if (hashClustersAreUsed && (clusterId != manualIndexCluster && clusterId != internalCluster && clusterId != indexCluster))
        storeHashClusterRecord(new ORecordId(rid));
      else
        storeLocalClusterRecord();

      if (!record.getIdentity().toString().equals(rid))
        throw new OSchemaException("Imported record '" + record.getIdentity() + "' has rid different from the original: " + rid);
    } catch (Exception t) {
      if (record != null)
        System.err.println("Error importing record " + record.getIdentity() + ". Source line " + jsonReader.getLineNumber()
            + ", column " + jsonReader.getColumnNumber());
      else
        System.err.println("Error importing record. Source line " + jsonReader.getLineNumber() + ", column "
            + jsonReader.getColumnNumber());

      throw t;
    } finally {
      jsonReader.readNext(OJSONReader.NEXT_IN_ARRAY);
    }

    return record.getIdentity();
  }

  private void storeLocalClusterRecord() {
    long nextAvailablePos = database.getStorage().getClusterDataRange(record.getIdentity().getClusterId())[1].longValue() + 1;

    // SAVE THE RECORD
    if (record.getIdentity().getClusterPosition().longValue() < nextAvailablePos) {
      // REWRITE PREVIOUS RECORD WITH THE SAME VERSION, SO USE A NEGATIVE NUMBER
      record.getRecordVersion().setRollbackMode();

      if (record instanceof ODocument)
        record.save();
      else
        ((ODatabaseRecord) database.getUnderlying()).save(record);
    } else {
      String clusterName = database.getClusterNameById(record.getIdentity().getClusterId());

      if (record.getIdentity().getClusterPosition().longValue() > nextAvailablePos) {
        // CREATE HOLES
        int holes = (int) (record.getIdentity().getClusterPosition().longValue() - nextAvailablePos);

        ODocument tempRecord = new ODocument();
        for (int i = 0; i < holes; ++i) {
          tempRecord.reset();
          ((ODatabaseRecord) database.getUnderlying()).save(tempRecord, clusterName);
          recordToDelete.add(tempRecord.getIdentity().toString());
        }
      }

      // APPEND THE RECORD
      record.setIdentity(-1, ORecordId.CLUSTER_POS_INVALID);
      if (record instanceof ODocument)
        record.save(clusterName);
      else
        ((ODatabaseRecord) database.getUnderlying()).save(record, clusterName);
    }
  }

  private void storeHashClusterRecord(final ORecordId rid) {
    ORecordInternal<?> recordInternal = database.load(rid);
    if (recordInternal != null)
      recordInternal.delete();

    record.setIdentity(rid);
    record.save(true);
  }

  private void importIndexes() throws IOException, ParseException {
    listener.onMessage("\nImporting indexes ...");
    database.load(new ORecordId(indexMgrRecordId)).clear().save();

    OIndexManagerProxy indexManager = database.getMetadata().getIndexManager();
    indexManager.reload();

    jsonReader.readNext(OJSONReader.BEGIN_COLLECTION);

    int n = 0;
    while (jsonReader.lastChar() != ']') {
      jsonReader.readNext(OJSONReader.BEGIN_OBJECT);

      String indexName = null;
      String indexType = null;
      Set<String> clustersToIndex = new HashSet<String>();
      OIndexDefinition indexDefinition = null;

      while (jsonReader.lastChar() != '}') {
        final String fieldName = jsonReader.readString(OJSONReader.FIELD_ASSIGNMENT);
        if (fieldName.equals("name"))
          indexName = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);
        else if (fieldName.equals("type"))
          indexType = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);
        else if (fieldName.equals("clustersToIndex"))
          clustersToIndex = importClustersToIndex();
        else if (fieldName.equals("definition"))
          indexDefinition = importIndexDefinition();
      }

      jsonReader.readNext(OJSONReader.NEXT_IN_ARRAY);

      listener.onMessage("\n- Index '" + indexName + "'...");
      // drop automatically created indexes
      indexManager.dropIndex(indexName);

      int[] clusterIdsToIndex = new int[clustersToIndex.size()];

      int i = 0;
      for (final String clusterName : clustersToIndex) {
        clusterIdsToIndex[i] = database.getClusterIdByName(clusterName);
        i++;
      }

      indexManager.createIndex(indexName, indexType, indexDefinition, clusterIdsToIndex, null);
      n++;
      listener.onMessage("OK");
    }

    listener.onMessage("\nDone. Created " + n + " indexes.");
    jsonReader.readNext(OJSONReader.NEXT_IN_OBJECT);
  }

  private Set<String> importClustersToIndex() throws IOException, ParseException {
    final Set<String> clustersToIndex = new HashSet<String>();

    jsonReader.readNext(OJSONReader.BEGIN_COLLECTION);

    while (jsonReader.lastChar() != ']') {
      final String clusterToIndex = jsonReader.readString(OJSONReader.NEXT_IN_ARRAY);
      clustersToIndex.add(clusterToIndex);
    }

    jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);
    return clustersToIndex;
  }

  private OIndexDefinition importIndexDefinition() throws IOException, ParseException {
    jsonReader.readString(OJSONReader.BEGIN_OBJECT);
    jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT);

    final String className = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);

    jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT);

    final String value = jsonReader.readString(OJSONReader.END_OBJECT, true);

    final OIndexDefinition indexDefinition;
    final ODocument indexDefinitionDoc = (ODocument) ORecordSerializerJSON.INSTANCE.fromString(value, null, null);
    try {
      final Class<?> indexDefClass = Class.forName(className);
      indexDefinition = (OIndexDefinition) indexDefClass.getDeclaredConstructor().newInstance();
      indexDefinition.fromStream(indexDefinitionDoc);
    } catch (final ClassNotFoundException e) {
      throw new IOException("Error during deserialization of index definition", e);
    } catch (final NoSuchMethodException e) {
      throw new IOException("Error during deserialization of index definition", e);
    } catch (final InvocationTargetException e) {
      throw new IOException("Error during deserialization of index definition", e);
    } catch (final InstantiationException e) {
      throw new IOException("Error during deserialization of index definition", e);
    } catch (final IllegalAccessException e) {
      throw new IOException("Error during deserialization of index definition", e);
    }

    jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);

    return indexDefinition;
  }

  public void close() {
    database.declareIntent(null);
  }
}
