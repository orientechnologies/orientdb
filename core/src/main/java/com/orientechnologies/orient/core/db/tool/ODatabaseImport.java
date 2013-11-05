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
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.ODatabase.STATUS;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OClassTrigger;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexManagerProxy;
import com.orientechnologies.orient.core.index.ORuntimeKeyIndexDefinition;
import com.orientechnologies.orient.core.index.OSimpleKeyIndexDefinition;
import com.orientechnologies.orient.core.index.hashindex.local.OMurmurHash3HashFunction;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OPropertyImpl;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OSecurityShared;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OJSONReader;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerJSON;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;
import com.orientechnologies.orient.core.type.tree.provider.OMVRBTreeRIDProvider;
import com.orientechnologies.orient.core.version.OVersionFactory;

/**
 * Import data from a file into a database.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ODatabaseImport extends ODatabaseImpExpAbstract {
  public static final String         EXPORT_IMPORT_MAP_NAME = "___exportImportRIDMap";

  private Map<OPropertyImpl, String> linkedClasses          = new HashMap<OPropertyImpl, String>();
  private Map<OClass, String>        superClasses           = new HashMap<OClass, String>();
  private OJSONReader                jsonReader;
  private ORecordInternal<?>         record;
  private boolean                    schemaImported         = false;
  private int                        exporterVersion        = -1;
  private ORID                       schemaRecordId;
  private ORID                       indexMgrRecordId;

  private boolean                    deleteRIDMapping       = true;

  private OIndex<OIdentifiable>      exportImportHashTable;

  private boolean                    preserveClusterIDs     = true;

  private Set<String>                indexesToRebuild       = new HashSet<String>();

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

    OMurmurHash3HashFunction<OIdentifiable> keyHashFunction = new OMurmurHash3HashFunction<OIdentifiable>();
    keyHashFunction.setValueSerializer(OLinkSerializer.INSTANCE);

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

  @Override
  protected void parseSetting(final String option, final List<String> items) {
    if (option.equalsIgnoreCase("-deleteRIDMapping"))
      deleteRIDMapping = Boolean.parseBoolean(items.get(0));
    else if (option.equalsIgnoreCase("-preserveClusterIDs"))
      preserveClusterIDs = Boolean.parseBoolean(items.get(0));
    else
      super.parseSetting(option, items);
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

      database.setStatus(STATUS.IMPORTING);

      for (OIndex index : database.getMetadata().getIndexManager().getIndexes()) {
        if (index.isAutomatic())
          indexesToRebuild.add(index.getName().toLowerCase());
      }

      removeDefaultNonSecurityClasses();

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

      listener.onMessage("\nRebuild of stale indexes...");
      for (String indexName : indexesToRebuild) {
        listener.onMessage("\nStart rebuild index " + indexName);
        database.command(new OCommandSQL("rebuild index " + indexName)).execute();
        listener.onMessage("\nRebuild  of index " + indexName + " is completed.");
      }
      listener.onMessage("\nStale indexes were rebuilt...");

      database.getStorage().synch();
      database.setStatus(STATUS.OPEN);

      if (isDeleteRIDMapping())
        removeExportImportRIDsMap();

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

  private void removeDefaultNonSecurityClasses() {
    listener.onMessage("\nRemoving all default non security classes");

    OSchema schema = database.getMetadata().getSchema();
    Collection<OClass> classes = schema.getClasses();

    final AbstractList<String> classesSortedByInheritance = new ArrayList<String>();
    for (OClass dbClass : classes) {
      classesSortedByInheritance.add(dbClass.getName());
    }

    for (OClass dbClass : classes) {
      OClass parentClass = dbClass.getSuperClass();
      if (parentClass != null) {
        classesSortedByInheritance.remove(dbClass.getName());
        final int parentIndex = classesSortedByInheritance.indexOf(parentClass.getName());
        classesSortedByInheritance.add(parentIndex, dbClass.getName());
      }
    }

    for (String className : classesSortedByInheritance) {
      if (!className.equalsIgnoreCase(ORole.CLASS_NAME) && !className.equalsIgnoreCase(OUser.CLASS_NAME)
          && !className.equalsIgnoreCase(OSecurityShared.IDENTITY_CLASSNAME)) {
        schema.dropClass(className);
        listener.onMessage("\nClass " + className + " was removed.");
      }
    }

    schema.save();
    schema.reload();

    listener.onMessage("\nRemoving all default non security classes ... DONE.");
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

    OIndexManagerProxy indexManager = database.getMetadata().getIndexManager();
    // FORCE RELOADING
    indexManager.reload();

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

        if (!value.isEmpty() && !indexName.equalsIgnoreCase(EXPORT_IMPORT_MAP_NAME)) {
          doc = (ODocument) ORecordSerializerJSON.INSTANCE.fromString(value, doc, null);
          doc.setLazyLoad(false);

          final OIdentifiable oldRid = doc.<OIdentifiable> field("rid");
          final OIdentifiable newRid;
          if (!doc.<Boolean> field("binary")) {
            if (exportImportHashTable != null)
              newRid = exportImportHashTable.get(oldRid);
            else
              newRid = oldRid;

            index.put(doc.field("key"), newRid != null ? newRid.getIdentity() : oldRid.getIdentity());
          } else {
            ORuntimeKeyIndexDefinition<?> runtimeKeyIndexDefinition = (ORuntimeKeyIndexDefinition<?>) index.getDefinition();
            OBinarySerializer<?> binarySerializer = runtimeKeyIndexDefinition.getSerializer();

            if (exportImportHashTable != null)
              newRid = exportImportHashTable.get(doc.<OIdentifiable> field("rid")).getIdentity();
            else
              newRid = doc.<OIdentifiable> field("rid");

            index.put(binarySerializer.deserialize(doc.<byte[]> field("key"), 0), newRid != null ? newRid : oldRid);
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

    boolean makeFullCheckPointAfterClusterCreation = false;
    if (database.getStorage() instanceof OLocalPaginatedStorage) {
      makeFullCheckPointAfterClusterCreation = ((OLocalPaginatedStorage) database.getStorage())
          .isMakeFullCheckPointAfterClusterCreate();
      ((OLocalPaginatedStorage) database.getStorage()).disableFullCheckPointAfterClusterCreate();
    }

    boolean recreateManualIndex = false;
    if (exporterVersion <= 4) {
      removeDefaultClusters();
      recreateManualIndex = true;
    }

    final Set<String> indexesToRebuild = new HashSet<String>();

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
        if (!preserveClusterIDs)
          clusterId = database.addCluster(type, name, null, null);
        else {
          clusterId = database.addCluster(type, name, id, null, null);
          assert clusterId == id;
        }

      }

      if (clusterId != id) {
        if (!preserveClusterIDs) {
          if (database.countClusterElements(clusterId - 1) == 0) {
            listener.onMessage("Found previous version: migrating old clusters...");
            database.dropCluster(name, true);
            database.addCluster(type, "temp_" + clusterId, null, null);
            clusterId = database.addCluster(type, name, null, null);
          } else
            throw new OConfigurationException("Imported cluster '" + name + "' has id=" + clusterId
                + " different from the original: " + id + ". To continue the import drop the cluster '"
                + database.getClusterNameById(clusterId - 1) + "' that has " + database.countClusterElements(clusterId - 1)
                + " records");
        } else {
          database.dropCluster(clusterId, false);
          database.addCluster(type, name, id, null, null);
        }
      }

      if (name != null
          && !(name.equalsIgnoreCase(OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME)
              || name.equalsIgnoreCase(OMetadataDefault.CLUSTER_INTERNAL_NAME) || name
                .equalsIgnoreCase(OMetadataDefault.CLUSTER_INDEX_NAME))) {
        database.command(new OCommandSQL("truncate cluster " + name)).execute();

        for (OIndex existingIndex : database.getMetadata().getIndexManager().getIndexes()) {
          if (existingIndex.getClusters().contains(name)) {
            indexesToRebuild.add(existingIndex.getName());
          }
        }
      }

      listener.onMessage("OK, assigned id=" + clusterId);

      total++;

      jsonReader.readNext(OJSONReader.NEXT_IN_ARRAY);
    }
    jsonReader.readNext(OJSONReader.COMMA_SEPARATOR);

    listener.onMessage("\nRebuilding indexes of truncated clusters ...");

    for (final String indexName : indexesToRebuild)
      database.getMetadata().getIndexManager().getIndex(indexName).rebuild(new OProgressListener() {
        @Override
        public void onBegin(Object iTask, long iTotal) {
          listener.onMessage("\nCluster content was truncated and index " + indexName + " will be rebuilt");
        }

        @Override
        public boolean onProgress(Object iTask, long iCounter, float iPercent) {
          listener.onMessage(String.format("\nIndex %s is rebuilt on %f percent", indexName, iPercent));
          return true;
        }

        @Override
        public void onCompletition(Object iTask, boolean iSucceed) {
          listener.onMessage("\nIndex " + indexName + " was successfully rebuilt.");
        }
      });

    listener.onMessage("\nDone " + indexesToRebuild.size() + " indexes were rebuilt.");

    if (recreateManualIndex) {
      database.addCluster(OStorage.CLUSTER_TYPE.PHYSICAL.toString(), OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME, null, null);
      database.getMetadata().getIndexManager().create();

      listener.onMessage("\nManual index cluster was recreated.");
    }

    listener.onMessage("\nDone. Imported " + total + " clusters");

    if (database.load(new ORecordId(database.getStorage().getConfiguration().indexMgrRecordId)) == null) {
      ODocument indexDocument = new ODocument();
      indexDocument.save(OMetadataDefault.CLUSTER_INTERNAL_NAME);

      database.getStorage().getConfiguration().indexMgrRecordId = indexDocument.getIdentity().toString();
      database.getStorage().getConfiguration().update();
    }

    if (database.getStorage() instanceof OLocalPaginatedStorage && makeFullCheckPointAfterClusterCreation)
      ((OLocalPaginatedStorage) database.getStorage()).enableFullCheckPointAfterClusterCreate();

    return total;
  }

  protected void removeDefaultClusters() {
    listener.onMessage("\nWARN: Exported database does not support manual index separation."
        + " Manual index cluster will be dropped.");

    // In v4 new cluster for manual indexes has been implemented. To keep database consistent we should shift back
    // all clusters and recreate cluster for manual indexes in the end.
    database.dropCluster(OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME, true);

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

    database.getMetadata().getIndexManager().dropIndex(EXPORT_IMPORT_MAP_NAME);
    exportImportHashTable = (OIndex<OIdentifiable>) database
        .getMetadata()
        .getIndexManager()
        .createIndex(EXPORT_IMPORT_MAP_NAME, OClass.INDEX_TYPE.DICTIONARY_HASH_INDEX.toString(),
            new OSimpleKeyIndexDefinition(OType.LINK), null, null);

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

    rewriteLinksInImportedDocuments();

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
        int indexId = database.getClusterIdByName(OMetadataDefault.CLUSTER_INDEX_NAME);

        if (record.getIdentity().getClusterId() == indexId || record.getIdentity().getClusterId() == oridsId)
          // JUMP INDEX RECORDS
          return null;
      }

      final int manualIndexCluster = database.getClusterIdByName(OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME);
      final int internalCluster = database.getClusterIdByName(OMetadataDefault.CLUSTER_INTERNAL_NAME);
      final int indexCluster = database.getClusterIdByName(OMetadataDefault.CLUSTER_INDEX_NAME);

      if (exporterVersion >= 4) {
        if (record.getIdentity().getClusterId() == manualIndexCluster)
          // JUMP INDEX RECORDS
          return null;
      }

      if (record.getIdentity().equals(indexMgrRecordId))
        return null;

      final ORID rid = record.getIdentity();

      final int clusterId = rid.getClusterId();

      if ((clusterId != manualIndexCluster && clusterId != internalCluster && clusterId != indexCluster)) {
        record.getRecordVersion().copyFrom(OVersionFactory.instance().createVersion());
        record.setDirty();
        record.setIdentity(new ORecordId());

        if (!preserveRids && record instanceof ODocument && ((ODocument) record).getSchemaClass() != null)
          record.save();
        else
          record.save(database.getClusterNameById(clusterId));

        if (!rid.equals(record.getIdentity()))
          // SAVE IT ONLY IF DIFFERENT
          exportImportHashTable.put(rid, record.getIdentity());
      }

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

  private void importIndexes() throws IOException, ParseException {
    listener.onMessage("\nImporting indexes ...");

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

      // drop automatically created indexes
      if (!indexName.equalsIgnoreCase(EXPORT_IMPORT_MAP_NAME)) {
        listener.onMessage("\n- Index '" + indexName + "'...");

        indexManager.dropIndex(indexName);
        indexesToRebuild.remove(indexName.toLowerCase());

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

  private void rewriteLinksInImportedDocuments() throws IOException {
    listener.onMessage("\nLinks are going to be updated according to new RIDs:");

    long totalDocuments = 0;
    Collection<String> clusterNames = database.getClusterNames();
    for (String clusterName : clusterNames) {
      if (OMetadataDefault.CLUSTER_INDEX_NAME.equals(clusterName) || OMetadataDefault.CLUSTER_INTERNAL_NAME.equals(clusterName)
          || OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME.equals(clusterName))
        continue;

      long documents = 0;

      listener.onMessage("\n- Cluster " + clusterName + "...");

      int clusterId = database.getClusterIdByName(clusterName);
      OStorage storage = database.getStorage();

      OPhysicalPosition[] positions = storage.ceilingPhysicalPositions(clusterId, new OPhysicalPosition(
          OClusterPositionFactory.INSTANCE.valueOf(0)));
      while (positions.length > 0) {
        for (OPhysicalPosition position : positions) {
          ORecord<?> record = database.load(new ORecordId(clusterId, position.clusterPosition));
          if (record instanceof ODocument) {
            ODocument document = (ODocument) record;
            rewriteLinksInDocument(document);

            documents++;
            totalDocuments++;

            if (documents % 10000 == 0)
              listener.onMessage("\n" + documents + " documents were processed...");
          }
        }

        positions = storage.higherPhysicalPositions(clusterId, positions[positions.length - 1]);
      }
      listener.onMessage(" Processed: " + documents);
    }

    listener.onMessage("\nTotal links updated: " + totalDocuments);
  }

  private void rewriteLinksInDocument(ODocument document) {
    RewritersFactory.INSTANCE.setExportImportHashTable(exportImportHashTable);

    DocumentRewriter documentRewriter = new DocumentRewriter();
    document = documentRewriter.rewriteValue(document);
    if (document != null)
      document.save();
  }

  public ODatabaseImport removeExportImportRIDsMap() {
    listener.onMessage("\nDeleting RID Mapping table...");
    if (exportImportHashTable != null) {
      database.command(new OCommandSQL("drop index " + EXPORT_IMPORT_MAP_NAME));
      exportImportHashTable = null;
    }

    listener.onMessage("OK\n");
    return this;
  }

  public void close() {
    database.declareIntent(null);
  }

  public boolean isDeleteRIDMapping() {
    return deleteRIDMapping;
  }

  public void setDeleteRIDMapping(boolean deleteRIDMapping) {
    this.deleteRIDMapping = deleteRIDMapping;
  }

  public void setPreserveClusterIDs(boolean preserveClusterIDs) {
    this.preserveClusterIDs = preserveClusterIDs;
  }

  private static class RewritersFactory {
    public static final RewritersFactory INSTANCE = new RewritersFactory();

    private OIndex<OIdentifiable>        exportImportHashTable;

    @SuppressWarnings("unchecked")
    public <T> FieldRewriter<T> findRewriter(ODocument document, String fieldName, T value) {
      if (value == null)
        return new IdentityRewriter<T>();

      OType fieldType = null;

      if (document != null) {
        OClass docClass = document.getSchemaClass();
        if (docClass != null) {
          OProperty property = docClass.getProperty(fieldName);
          if (property != null)
            fieldType = property.getType();
        } else {
          fieldType = document.fieldType(fieldName);
        }
      }

      if (fieldType == null) {
        if (value instanceof ODocument)
          return (FieldRewriter<T>) new DocumentRewriter();
        else if (value instanceof List)
          return (FieldRewriter<T>) new ListRewriter();
        else if (value instanceof Map)
          return (FieldRewriter<T>) new MapRewriter();
        else if (value instanceof OMVRBTreeRIDSet)
          return (FieldRewriter<T>) new LinkSetRewriter();
        else if (value instanceof ORID)
          return (FieldRewriter<T>) new LinkRewriter(exportImportHashTable);
        else if (value instanceof Set)
          return (FieldRewriter<T>) new SetRewriter();
        else
          return new IdentityRewriter<T>();
      }

      switch (fieldType) {
      case EMBEDDED:
        return (FieldRewriter<T>) new DocumentRewriter();
      case LINKLIST:
        return (FieldRewriter<T>) new ListRewriter();
      case LINKMAP:
        return (FieldRewriter<T>) new MapRewriter();
      case LINKSET:
        return (FieldRewriter<T>) new LinkSetRewriter();
      case LINK:
        return (FieldRewriter<T>) new LinkRewriter(exportImportHashTable);
      case EMBEDDEDLIST:
        return (FieldRewriter<T>) new ListRewriter();
      case EMBEDDEDMAP:
        return (FieldRewriter<T>) new MapRewriter();
      case EMBEDDEDSET:
        return (FieldRewriter<T>) new SetRewriter();
      }

      return new IdentityRewriter<T>();
    }

    private void setExportImportHashTable(OIndex<OIdentifiable> exportImportHashTable) {
      this.exportImportHashTable = exportImportHashTable;
    }
  }

  private interface FieldRewriter<T> {
    T rewriteValue(T value);
  }

  private static class IdentityRewriter<T> implements FieldRewriter<T> {
    @Override
    public T rewriteValue(T value) {
      return null;
    }
  }

  private static class ListRewriter implements FieldRewriter<List<?>> {
    @Override
    public List<?> rewriteValue(List<?> listValue) {
      boolean wasRewritten = false;
      List<Object> result = new ArrayList<Object>(listValue.size());
      for (Object listItem : listValue) {
        FieldRewriter<Object> fieldRewriter = RewritersFactory.INSTANCE.findRewriter(null, null, listItem);
        Object rewrittenItem = fieldRewriter.rewriteValue(listItem);
        if (rewrittenItem != null) {
          wasRewritten = true;
          result.add(rewrittenItem);
        } else
          result.add(listItem);
      }

      if (!wasRewritten)
        return null;

      return result;
    }
  }

  private static class DocumentRewriter implements FieldRewriter<ODocument> {
    @Override
    public ODocument rewriteValue(ODocument documentValue) {
      boolean wasRewritten = false;
      documentValue.setLazyLoad(false);

      for (String fieldName : documentValue.fieldNames()) {
        Object fieldValue = documentValue.field(fieldName);

        FieldRewriter<Object> fieldRewriter = RewritersFactory.INSTANCE.findRewriter(documentValue, fieldName, fieldValue);
        Object newFieldValue = fieldRewriter.rewriteValue(fieldValue);

        if (newFieldValue != null) {
          documentValue.field(fieldName, newFieldValue);
          wasRewritten = true;
        }
      }

      if (wasRewritten)
        return documentValue;

      return null;
    }
  }

  private static class MapRewriter implements FieldRewriter<Map<String, Object>> {
    @Override
    public Map<String, Object> rewriteValue(Map<String, Object> mapValue) {
      boolean wasRewritten = false;
      Map<String, Object> result = new HashMap<String, Object>();
      for (Map.Entry<String, Object> entry : mapValue.entrySet()) {
        String key = entry.getKey();
        Object value = entry.getValue();

        FieldRewriter<Object> fieldRewriter = RewritersFactory.INSTANCE.findRewriter(null, null, value);
        Object newValue = fieldRewriter.rewriteValue(value);

        if (newValue != null) {
          result.put(key, newValue);
          wasRewritten = true;
        } else
          result.put(key, value);
      }

      if (wasRewritten)
        return result;

      return null;
    }
  }

  private static class LinkSetRewriter implements FieldRewriter<OMVRBTreeRIDSet> {

    @Override
    public OMVRBTreeRIDSet rewriteValue(OMVRBTreeRIDSet setValue) {
      setValue.setAutoConvertToRecord(false);

      OMVRBTreeRIDSet result = new OMVRBTreeRIDSet();
      result.setAutoConvertToRecord(false);

      boolean wasRewritten = false;
      for (OIdentifiable identifiable : setValue) {
        FieldRewriter<ORID> fieldRewriter = RewritersFactory.INSTANCE.findRewriter(null, null, identifiable.getIdentity());
        ORID newRid = fieldRewriter.rewriteValue(identifiable.getIdentity());

        if (newRid != null) {
          wasRewritten = true;
          result.add(newRid);
        } else
          result.add(identifiable);
      }

      if (wasRewritten)
        return result;

      result.clear();
      return null;
    }
  }

  private static class SetRewriter implements FieldRewriter<Set<?>> {
    @Override
    public Set<?> rewriteValue(Set<?> setValue) {
      boolean wasRewritten = false;
      Set<Object> result = new HashSet<Object>();
      for (Object item : setValue) {
        FieldRewriter<Object> fieldRewriter = RewritersFactory.INSTANCE.findRewriter(null, null, item);
        Object newItem = fieldRewriter.rewriteValue(item);

        if (newItem != null) {
          wasRewritten = true;
          result.add(newItem);
        } else
          result.add(item);
      }

      if (wasRewritten)
        return result;

      return null;
    }
  }

  private static class LinkRewriter implements FieldRewriter<ORID> {
    private final OIndex<OIdentifiable> exportImportHashTable;

    private LinkRewriter(OIndex<OIdentifiable> exportImportHashTable) {
      this.exportImportHashTable = exportImportHashTable;
    }

    @Override
    public ORID rewriteValue(ORID value) {
      if (!value.isPersistent())
        return null;

      final OIdentifiable result = exportImportHashTable.get(value);

      return result != null ? result.getIdentity() : null;
    }
  }
}
