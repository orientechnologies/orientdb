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
package com.orientechnologies.orient.core.db.tool;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.ODatabase.STATUS;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODocumentFieldVisitor;
import com.orientechnologies.orient.core.db.document.ODocumentFieldWalker;
import com.orientechnologies.orient.core.db.record.OClassTrigger;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexFactory;
import com.orientechnologies.orient.core.index.OIndexManagerProxy;
import com.orientechnologies.orient.core.index.OIndexes;
import com.orientechnologies.orient.core.index.ORuntimeKeyIndexDefinition;
import com.orientechnologies.orient.core.index.OSimpleKeyIndexDefinition;
import com.orientechnologies.orient.core.index.hashindex.local.OMurmurHash3HashFunction;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.schema.OPropertyImpl;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.OIdentity;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OSecurityShared;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
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

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.zip.GZIPInputStream;

/**
 * Import data from a file into a database.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ODatabaseImport extends ODatabaseImpExpAbstract {
  public static final String         EXPORT_IMPORT_MAP_NAME          = "___exportImportRIDMap";
  public static final int            IMPORT_RECORD_DUMP_LAP_EVERY_MS = 5000;

  private Map<OPropertyImpl, String> linkedClasses                   = new HashMap<OPropertyImpl, String>();
  private Map<OClass, List<String>>  superClasses                    = new HashMap<OClass, List<String>>();
  private OJSONReader                jsonReader;
  private ORecord                    record;
  private boolean                    schemaImported                  = false;
  private int                        exporterVersion                 = -1;
  private ORID                       schemaRecordId;
  private ORID                       indexMgrRecordId;

  private boolean                    deleteRIDMapping                = true;

  private OIndex<OIdentifiable>      exportImportHashTable;

  private boolean                    preserveClusterIDs              = true;
  private boolean                    migrateLinks                    = true;
  private boolean                    merge                           = false;
  private boolean                    rebuildIndexes                  = true;

  private Set<String>                indexesToRebuild                = new HashSet<String>();
  private Map<String, String>        convertedClassNames             = new HashMap<String, String>();

  private interface ValuesConverter<T> {
    T convert(T value);
  }

  private static final class ConvertersFactory {
    public static final ConvertersFactory INSTANCE = new ConvertersFactory();

    public ValuesConverter getConverter(Object value) {
      if (value instanceof Map)
        return MapConverter.INSTANCE;

      if (value instanceof List)
        return ListConverter.INSTANCE;

      if (value instanceof Set)
        return SetConverter.INSTANCE;

      if (value instanceof ORidBag)
        return RidBagConverter.INSTANCE;

      if (value instanceof OIdentifiable)
        return LinkConverter.INSTANCE;

      return null;
    }
  }

  private static final class LinksRewriter implements ODocumentFieldVisitor {
    @Override
    public Object visitField(OType type, OType linkedType, Object value) {
      boolean oldAutoConvertValue = false;
      if (value instanceof ORecordLazyMultiValue) {
        ORecordLazyMultiValue multiValue = (ORecordLazyMultiValue) value;
        oldAutoConvertValue = multiValue.isAutoConvertToRecord();
        multiValue.setAutoConvertToRecord(false);
      }

      final ValuesConverter valuesConverter = ConvertersFactory.INSTANCE.getConverter(value);
      if (valuesConverter == null)
        return value;

      final Object newValue = valuesConverter.convert(value);

      if (value instanceof ORecordLazyMultiValue) {
        ORecordLazyMultiValue multiValue = (ORecordLazyMultiValue) value;
        multiValue.setAutoConvertToRecord(oldAutoConvertValue);
      }

      return newValue;
    }

    @Override
    public boolean goFurther(OType type, OType linkedType, Object value, Object newValue) {
      return true;
    }

    @Override
    public boolean goDeeper(OType type, OType linkedType, Object value) {
      return true;
    }

    @Override
    public boolean updateMode() {
      return true;
    }

  }

  private static abstract class AbstractCollectionConverter<T> implements ValuesConverter<T> {
    interface ResultCallback {
      void add(Object item);
    }

    protected boolean convertSingleValue(final Object item, ResultCallback result, boolean updated) {
      if (item == null)
        return false;

      if (item instanceof OIdentifiable) {
        final ValuesConverter<OIdentifiable> converter = (ValuesConverter<OIdentifiable>) ConvertersFactory.INSTANCE
            .getConverter(item);

        final OIdentifiable newValue = converter.convert((OIdentifiable) item);
        result.add(newValue);

        if (!newValue.equals(item))
          updated = true;
      } else {
        final ValuesConverter valuesConverter = ConvertersFactory.INSTANCE.getConverter(item.getClass());
        if (valuesConverter == null)
          result.add(item);
        else {
          final Object newValue = valuesConverter.convert(item);
          if (newValue != item)
            updated = true;

          result.add(newValue);
        }
      }

      return updated;
    }
  }

  private static final class SetConverter extends AbstractCollectionConverter<Set> {
    public static final SetConverter INSTANCE = new SetConverter();

    @Override
    public Set convert(Set value) {
      boolean updated = false;
      final Set result;

      if (value instanceof OMVRBTreeRIDSet) {
        OMVRBTreeRIDSet ridSet = new OMVRBTreeRIDSet();
        ridSet.setAutoConvertToRecord(false);

        result = ridSet;
      } else
        result = new HashSet();

      final ResultCallback callback = new ResultCallback() {
        @Override
        public void add(Object item) {
          result.add(item);
        }
      };

      for (Object item : value)
        updated = convertSingleValue(item, callback, updated);

      if (updated)
        return result;

      return value;
    }
  }

  private static final class ListConverter extends AbstractCollectionConverter<List> {
    public static final ListConverter INSTANCE = new ListConverter();

    @Override
    public List convert(List value) {
      final List result = new ArrayList();

      final ResultCallback callback = new ResultCallback() {
        @Override
        public void add(Object item) {
          result.add(item);
        }
      };
      boolean updated = false;

      for (Object item : value)
        updated = convertSingleValue(item, callback, updated);

      if (updated)
        return result;

      return value;
    }
  }

  private static final class RidBagConverter extends AbstractCollectionConverter<ORidBag> {
    public static final RidBagConverter INSTANCE = new RidBagConverter();

    @Override
    public ORidBag convert(ORidBag value) {
      final ORidBag result = new ORidBag();
      boolean updated = false;
      final ResultCallback callback = new ResultCallback() {
        @Override
        public void add(Object item) {
          result.add((OIdentifiable) item);
        }
      };

      for (OIdentifiable identifiable : value)
        updated = convertSingleValue(identifiable, callback, updated);

      if (updated)
        return result;

      return value;
    }
  }

  private static final class MapConverter extends AbstractCollectionConverter<Map> {
    public static final MapConverter INSTANCE = new MapConverter();

    @Override
    public Map convert(Map value) {
      final HashMap result = new HashMap();
      boolean updated = false;
      final class MapResultCallback implements ResultCallback {
        private Object key;

        @Override
        public void add(Object item) {
          result.put(key, item);
        }

        public void setKey(Object key) {
          this.key = key;
        }
      }

      final MapResultCallback callback = new MapResultCallback();
      for (Map.Entry entry : (Iterable<Map.Entry>) value.entrySet()) {
        callback.setKey(entry.getKey());
        updated = convertSingleValue(entry.getValue(), callback, updated);
      }
      if (updated)
        return result;

      return value;
    }
  }

  private static final class LinkConverter implements ValuesConverter<OIdentifiable> {
    public static final LinkConverter INSTANCE = new LinkConverter();

    private OIndex<OIdentifiable>     exportImportHashTable;

    @Override
    public OIdentifiable convert(OIdentifiable value) {
      final ORID rid = value.getIdentity();
      if (!rid.isPersistent())
        return value;

      final OIdentifiable newRid = exportImportHashTable.get(rid);
      if (newRid == null)
        return value;

      return newRid.getIdentity();
    }

    public void setExportImportHashTable(OIndex<OIdentifiable> exportImportHashTable) {
      this.exportImportHashTable = exportImportHashTable;
    }
  }

  public ODatabaseImport(final ODatabaseDocumentInternal database, final String iFileName, final OCommandOutputListener iListener)
      throws IOException {
    super(database, iFileName, iListener);

    InputStream inStream;
    final BufferedInputStream bf = new BufferedInputStream(new FileInputStream(fileName));
    bf.mark(1024);
    try {
      inStream = new GZIPInputStream(bf, 16384); // 16KB
    } catch (Exception e) {
      bf.reset();
      inStream = bf;
    }

    OMurmurHash3HashFunction<OIdentifiable> keyHashFunction = new OMurmurHash3HashFunction<OIdentifiable>();
    keyHashFunction.setValueSerializer(OLinkSerializer.INSTANCE);

    jsonReader = new OJSONReader(new InputStreamReader(inStream));
    database.declareIntent(new OIntentMassiveInsert());
  }

  public ODatabaseImport(final ODatabaseDocumentInternal database, final InputStream iStream, final OCommandOutputListener iListener)
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

      database.setMVCC(false);
      database.setValidationEnabled(false);

      database.setStatus(STATUS.IMPORTING);

      for (OIndex<?> index : database.getMetadata().getIndexManager().getIndexes()) {
        if (index.isAutomatic())
          indexesToRebuild.add(index.getName().toLowerCase());
      }

      if (!merge)
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

      if (rebuildIndexes)
        rebuildIndexes();

      // This is needed to insure functions loaded into an open
      // in memory database are available after the import.
      // see issue #5245
      database.getMetadata().reload();
      
      database.getStorage().synch();
      database.setStatus(STATUS.OPEN);

      if (isDeleteRIDMapping())
        removeExportImportRIDsMap();

      listener.onMessage("\n\nDatabase import completed in " + ((System.currentTimeMillis() - time)) + " ms");

    } catch (Exception e) {
      final StringWriter writer = new StringWriter();
      writer.append("Error on database import happened just before line " + jsonReader.getLineNumber() + ", column "
          + jsonReader.getColumnNumber() + "\n");
      final PrintWriter printWriter = new PrintWriter(writer);
      e.printStackTrace(printWriter);
      printWriter.flush();

      listener.onMessage(writer.toString());

      try {
        writer.close();
      } catch (IOException e1) {
        throw new ODatabaseExportException("Error on importing database '" + database.getName() + "' from file: " + fileName, e1);
      }

      throw new ODatabaseExportException("Error on importing database '" + database.getName() + "' from file: " + fileName, e);
    } finally {
      close();
    }

    return this;
  }

  public void rebuildIndexes() {
    database.getMetadata().getIndexManager().reload();

    OIndexManagerProxy indexManager = database.getMetadata().getIndexManager();

    listener.onMessage("\nRebuild of stale indexes...");
    for (String indexName : indexesToRebuild) {

      if (indexManager.getIndex(indexName) == null) {
        listener.onMessage("\nIndex " + indexName + " is skipped because it is absent in imported DB.");
        continue;
      }

      listener.onMessage("\nStart rebuild index " + indexName);
      database.command(new OCommandSQL("rebuild index " + indexName)).execute();
      listener.onMessage("\nRebuild  of index " + indexName + " is completed.");
    }
    listener.onMessage("\nStale indexes were rebuilt...");
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

  public boolean isMigrateLinks() {
    return migrateLinks;
  }

  public void setMigrateLinks(boolean migrateLinks) {
    this.migrateLinks = migrateLinks;
  }

  public boolean isRebuildIndexes() {
    return rebuildIndexes;
  }

  public void setRebuildIndexes(boolean rebuildIndexes) {
    this.rebuildIndexes = rebuildIndexes;
  }

  public boolean isPreserveClusterIDs() {
    return preserveClusterIDs;
  }

  public void setPreserveClusterIDs(boolean preserveClusterIDs) {
    this.preserveClusterIDs = preserveClusterIDs;
  }

  public boolean isMerge() {
    return merge;
  }

  public void setMerge(boolean merge) {
    this.merge = merge;
  }

  public boolean isDeleteRIDMapping() {
    return deleteRIDMapping;
  }

  public void setDeleteRIDMapping(boolean deleteRIDMapping) {
    this.deleteRIDMapping = deleteRIDMapping;
  }

  @Override
  protected void parseSetting(final String option, final List<String> items) {
    if (option.equalsIgnoreCase("-deleteRIDMapping"))
      deleteRIDMapping = Boolean.parseBoolean(items.get(0));
    else if (option.equalsIgnoreCase("-preserveClusterIDs"))
      preserveClusterIDs = Boolean.parseBoolean(items.get(0));
    else if (option.equalsIgnoreCase("-merge"))
      merge = Boolean.parseBoolean(items.get(0));
    else if (option.equalsIgnoreCase("-migrateLinks"))
      migrateLinks = Boolean.parseBoolean(items.get(0));
    else if (option.equalsIgnoreCase("-rebuildIndexes"))
      rebuildIndexes = Boolean.parseBoolean(items.get(0));
    else
      super.parseSetting(option, items);
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

    database.getStorage().setDefaultClusterId(database.addCluster(OStorage.CLUSTER_DEFAULT_NAME));

    // Starting from v4 schema has been moved to internal cluster.
    // Create a stub at #2:0 to prevent cluster position shifting.
    new ODocument().save(OStorage.CLUSTER_DEFAULT_NAME);

    database.getMetadata().getSecurity().create();
  }

  private void removeDefaultNonSecurityClasses() {
    listener.onMessage("\nNon merge mode (-merge=false): removing all default non security classes");

    OSchema schema = database.getMetadata().getSchema();
    Collection<OClass> classes = schema.getClasses();
    OClass orole = schema.getClass(ORole.CLASS_NAME);
    OClass ouser = schema.getClass(OUser.CLASS_NAME);
    OClass oidentity = schema.getClass(OIdentity.CLASS_NAME);
    final Map<String, OClass> classesToDrop = new HashMap<String, OClass>();
    for (OClass dbClass : classes) {
      String className = dbClass.getName();

      if (!dbClass.isSuperClassOf(orole) && !dbClass.isSuperClassOf(ouser) && !dbClass.isSuperClassOf(oidentity)) {
        classesToDrop.put(className, dbClass);
      }
    }

    int removedClasses = 0;
    while (!classesToDrop.isEmpty()) {
      final AbstractList<String> classesReadyToDrop = new ArrayList<String>();
      for (String className : classesToDrop.keySet()) {
        boolean isSuperClass = false;
        for (OClass dbClass : classesToDrop.values()) {
          List<OClass> parentClasses = dbClass.getSuperClasses();
          if (parentClasses != null) {
            for(OClass parentClass:parentClasses) {
              if (className.equalsIgnoreCase(parentClass.getName())) {
                isSuperClass = true;
                break;
              }
            }
          }
        }
        if (!isSuperClass) {
          classesReadyToDrop.add(className);
        }
      }
      for (String className : classesReadyToDrop) {
        schema.dropClass(className);
        classesToDrop.remove(className);
        removedClasses++;
        listener.onMessage("\n- Class " + className + " was removed.");
      }
    }

    schema.save();
    schema.reload();

    listener.onMessage("\nRemoved " + removedClasses + " classes.");
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

    listener.onMessage("\nDone. Imported " + String.format("%,d", n) + " indexes.");

    jsonReader.readNext(OJSONReader.NEXT_IN_OBJECT);
  }

  private void importSchema() throws IOException, ParseException {
    listener.onMessage("\nImporting database schema...");

    jsonReader.readNext(OJSONReader.BEGIN_OBJECT);
    @SuppressWarnings("unused")
    int schemaVersion = jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT).checkContent("\"version\"")
        .readNumber(OJSONReader.ANY_NUMBER, true);
    jsonReader.readNext(OJSONReader.COMMA_SEPARATOR);
    jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT);
    // This can be removed after the M1 expires
    if (jsonReader.getValue().equals("\"globalProperties\"")) {
      jsonReader.readNext(OJSONReader.BEGIN_COLLECTION);
      do {
        jsonReader.readNext(OJSONReader.BEGIN_OBJECT);
        jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT).checkContent("\"name\"");
        String name = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);
        jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT).checkContent("\"global-id\"");
        String id = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);
        jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT).checkContent("\"type\"");
        String type = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);
        // getDatabase().getMetadata().getSchema().createGlobalProperty(name, OType.valueOf(type), Integer.valueOf(id));
        jsonReader.readNext(OJSONReader.NEXT_IN_ARRAY);
      } while (jsonReader.lastChar() == ',');
      jsonReader.readNext(OJSONReader.COMMA_SEPARATOR);
      jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT);
    }

    jsonReader.checkContent("\"classes\"").readNext(OJSONReader.BEGIN_COLLECTION);

    long classImported = 0;

    try {
      do {
        jsonReader.readNext(OJSONReader.BEGIN_OBJECT);

        String className = jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT).checkContent("\"name\"")
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

        if (className.contains(".")) {
          // MIGRATE OLD NAME WITH . TO _
          final String newClassName = className.replace('.', '_');
          convertedClassNames.put(className, newClassName);

          listener.onMessage("\nWARNING: class '" + className + "' has been renamed in '" + newClassName + "'\n");

          className = newClassName;
        }

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
          } else if (value.equals("\"strictMode\"")) {
            final String strictMode = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);
            cls.setStrictMode(Boolean.parseBoolean(strictMode));
          } else if (value.equals("\"short-name\"")) {
            final String shortName = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);
            if( !cls.getName().equalsIgnoreCase( shortName ))
              cls.setShortName(shortName);
          } else if (value.equals("\"super-class\"")) {
            // @compatibility <2.1 SINGLE CLASS ONLY
            final String classSuper = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);
            final List<String> superClassNames = new ArrayList<String>();
            superClassNames.add(classSuper);
            superClasses.put(cls, superClassNames);
          } else if (value.equals("\"super-classes\"")) {
            // MULTIPLE CLASSES
            jsonReader.readNext(OJSONReader.BEGIN_COLLECTION);

            final List<String> superClassNames = new ArrayList<String>();
            while (jsonReader.lastChar() != ']') {
              jsonReader.readNext(OJSONReader.NEXT_IN_ARRAY);

              final String clsName = jsonReader.getValue();

              superClassNames.add(OStringSerializerHelper.getStringContent(clsName));
            }
            jsonReader.readNext(OJSONReader.NEXT_IN_OBJECT);

            superClasses.put(cls, superClassNames);
          } else if (value.equals("\"properties\"")) {
            // GET PROPERTIES
            jsonReader.readNext(OJSONReader.BEGIN_COLLECTION);

            while (jsonReader.lastChar() != ']') {
              importProperty(cls);

              if (jsonReader.lastChar() == '}')
                jsonReader.readNext(OJSONReader.NEXT_IN_ARRAY);
            }
            jsonReader.readNext(OJSONReader.END_OBJECT);
          } else if (value.equals("\"customFields\"")) {
            Map<String, String> customFields = importCustomFields();
            for (Entry<String, String> entry : customFields.entrySet()) {
              cls.setCustom(entry.getKey(), entry.getValue());
            }
          } else if (value.equals("\"cluster-selection\"")) {
            // @SINCE 1.7
            cls.setClusterSelection(jsonReader.readString(OJSONReader.NEXT_IN_OBJECT));
          }
        }

        classImported++;

        jsonReader.readNext(OJSONReader.NEXT_IN_ARRAY);
      } while (jsonReader.lastChar() == ',');

      // REBUILD ALL THE INHERITANCE
      for (Map.Entry<OClass, List<String>> entry : superClasses.entrySet())
        for (String s : entry.getValue()) {
          OClass superClass = database.getMetadata().getSchema().getClass(s);;
          if(!entry.getKey().getSuperClasses().contains(superClass))
            entry.getKey().addSuperClass(superClass);
        }

      // SET ALL THE LINKED CLASSES
      for (Map.Entry<OPropertyImpl, String> entry : linkedClasses.entrySet()) {
        entry.getKey().setLinkedClass(database.getMetadata().getSchema().getClass(entry.getValue()));
      }

      database.getMetadata().getSchema().save();

      if (exporterVersion < 11) {
        OClass role = database.getMetadata().getSchema().getClass("ORole");
        role.dropProperty("rules");
      }

      listener.onMessage("OK (" + classImported + " classes)");
      schemaImported = true;
      jsonReader.readNext(OJSONReader.END_OBJECT);
      jsonReader.readNext(OJSONReader.COMMA_SEPARATOR);
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on importing schema", e);
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
    String collate = null;

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
      else if (attrib.equals("\"collate\""))
        collate = value;
      else if (attrib.equals("\"customFields\""))
        customFields = importCustomFields();
    }

    OPropertyImpl prop = (OPropertyImpl) iClass.getProperty(propName);
    if (prop == null) {
      // CREATE IT
      prop = (OPropertyImpl) iClass.createProperty(propName, type);
    }
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
    if (collate != null)
      prop.setCollate(value);
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

      int id;
      if (exporterVersion < 9) {
        id = jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT).checkContent("\"id\"").readInteger(OJSONReader.COMMA_SEPARATOR);
        String type = jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT).checkContent("\"type\"")
            .readString(OJSONReader.NEXT_IN_OBJECT);
      } else
        id = jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT).checkContent("\"id\"").readInteger(OJSONReader.NEXT_IN_OBJECT);

      String type;
      if (jsonReader.lastChar() == ',')
        type = jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT).checkContent("\"type\"").readString(OJSONReader.NEXT_IN_OBJECT);
      else
        type = "PHYSICAL";

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
          clusterId = database.addCluster(name);
        else {
          clusterId = database.addCluster(name, id, null);
          assert clusterId == id;
        }
      }

      if (clusterId != id) {
        if (!preserveClusterIDs) {
          if (database.countClusterElements(clusterId - 1) == 0) {
            listener.onMessage("Found previous version: migrating old clusters...");
            database.dropCluster(name, true);
            database.addCluster("temp_" + clusterId, null);
            clusterId = database.addCluster(name);
          } else
            throw new OConfigurationException("Imported cluster '" + name + "' has id=" + clusterId
                + " different from the original: " + id + ". To continue the import drop the cluster '"
                + database.getClusterNameById(clusterId - 1) + "' that has " + database.countClusterElements(clusterId - 1)
                + " records");
        } else {
          database.dropCluster(clusterId, false);
          database.addCluster(name, id, null);
        }
      }

      if (name != null
          && !(name.equalsIgnoreCase(OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME)
              || name.equalsIgnoreCase(OMetadataDefault.CLUSTER_INTERNAL_NAME) || name
                .equalsIgnoreCase(OMetadataDefault.CLUSTER_INDEX_NAME))) {
        if (!merge)
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
        private long last = 0;

        @Override
        public void onBegin(Object iTask, long iTotal, Object metadata) {
          listener.onMessage("\n- Cluster content was updated: rebuilding index '" + indexName + "'...");
        }

        @Override
        public boolean onProgress(Object iTask, long iCounter, float iPercent) {
          final long now = System.currentTimeMillis();
          if (last == 0)
            last = now;
          else if (now - last > 1000) {
            listener.onMessage(String.format("\nIndex '%s' is rebuilding (%.2f/100)", indexName, iPercent));
            last = now;
          }
          return true;
        }

        @Override
        public void onCompletition(Object iTask, boolean iSucceed) {
          listener.onMessage(" Index " + indexName + " was successfully rebuilt.");
        }
      });

    listener.onMessage("\nDone " + indexesToRebuild.size() + " indexes were rebuilt.");

    if (recreateManualIndex) {
      database.addCluster(OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME);
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

  private long importRecords() throws Exception {
    long total = 0;

    database.getMetadata().getIndexManager().dropIndex(EXPORT_IMPORT_MAP_NAME);
    OIndexFactory factory = OIndexes.getFactory(OClass.INDEX_TYPE.DICTIONARY_HASH_INDEX.toString(), null);

    exportImportHashTable = (OIndex<OIdentifiable>) database
        .getMetadata()
        .getIndexManager()
        .createIndex(EXPORT_IMPORT_MAP_NAME, OClass.INDEX_TYPE.DICTIONARY_HASH_INDEX.toString(),
            new OSimpleKeyIndexDefinition(factory.getLastVersion(), OType.LINK), null, null, null);

    jsonReader.readNext(OJSONReader.BEGIN_COLLECTION);

    long totalRecords = 0;

    listener.onMessage("\n\nImporting records...");

    ORID rid;
    ORID lastRid = new ORecordId();
    final long begin = System.currentTimeMillis();
    long lastLapRecords = 0;
    long last = begin;
    Set<String> involvedClusters = new HashSet<String>();

    while (jsonReader.lastChar() != ']') {
      rid = importRecord();

      if (rid != null) {
        ++lastLapRecords;
        ++totalRecords;

        if (rid.getClusterId() != lastRid.getClusterId() || involvedClusters.isEmpty())
          involvedClusters.add(database.getClusterNameById(rid.getClusterId()));

        final long now = System.currentTimeMillis();
        if (now - last > IMPORT_RECORD_DUMP_LAP_EVERY_MS) {
          final List<String> sortedClusters = new ArrayList<String>(involvedClusters);
          Collections.sort(sortedClusters);

          listener.onMessage(String.format(
              "\n- Imported %,d records into clusters: %s. Total records imported so far: %,d (%,.2f/sec)", lastLapRecords,
              sortedClusters, totalRecords, (float) lastLapRecords * 1000 / (float) IMPORT_RECORD_DUMP_LAP_EVERY_MS));

          // RESET LAP COUNTERS
          last = now;
          lastLapRecords = 0;
          involvedClusters.clear();
        }
        lastRid = rid;
      }

      record = null;
    }

    if (migrateLinks)
      migrateLinksInImportedDocuments();

    listener.onMessage(String.format("\n\nDone. Imported %,d records in %,.2f secs\n", totalRecords,
        ((float) (System.currentTimeMillis() - begin)) / 1000));

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

      try {
        record = ORecordSerializerJSON.INSTANCE.fromString(value, record, null);
      } catch (OSerializationException e) {
        if (e.getCause() instanceof OSchemaException) {
          // EXTRACT CLASS NAME If ANY
          final int pos = value.indexOf("\"@class\":\"");
          if (pos > -1) {
            final int end = value.indexOf("\"", pos + "\"@class\":\"".length() + 1);
            final String value1 = value.substring(0, pos + "\"@class\":\"".length());
            final String clsName = value.substring(pos + "\"@class\":\"".length(), end);
            final String value2 = value.substring(end);

            final String newClassName = convertedClassNames.get(clsName);

            value = value1 + newClassName + value2;
            // OVERWRITE CLASS NAME WITH NEW NAME
            record = ORecordSerializerJSON.INSTANCE.fromString(value, record, null);
          }
        }
      }

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

      if (record.getIdentity().getClusterId() == 0 && record.getIdentity().getClusterPosition() == 1)
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
        ORecordInternal.setIdentity(record, new ORecordId());

        if (!preserveRids && record instanceof ODocument && ODocumentInternal.getImmutableSchemaClass(((ODocument) record)) != null)
          record.save();
        else
          record.save(database.getClusterNameById(clusterId));

        if (!rid.equals(record.getIdentity()))
          // SAVE IT ONLY IF DIFFERENT
          exportImportHashTable.put(rid, record.getIdentity());
      }

    } catch (Exception t) {
      if (record != null)
        OLogManager.instance().error(
            this,
            "Error importing record " + record.getIdentity() + ". Source line " + jsonReader.getLineNumber() + ", column "
                + jsonReader.getColumnNumber());
      else
        OLogManager.instance().error(this,
            "Error importing record. Source line " + jsonReader.getLineNumber() + ", column " + jsonReader.getColumnNumber());

      throw t;
    } finally {
      jsonReader.readNext(OJSONReader.NEXT_IN_ARRAY);
    }

    return record.getIdentity();
  }

  private void importIndexes() throws IOException, ParseException {
    listener.onMessage("\n\nImporting indexes ...");

    OIndexManagerProxy indexManager = database.getMetadata().getIndexManager();
    indexManager.reload();

    jsonReader.readNext(OJSONReader.BEGIN_COLLECTION);

    int n = 0;
    while (jsonReader.lastChar() != ']') {
      jsonReader.readNext(OJSONReader.BEGIN_OBJECT);

      String blueprintsIndexClass = null;
      String indexName = null;
      String indexType = null;
      String indexAlgorithm = null;
      Set<String> clustersToIndex = new HashSet<String>();
      OIndexDefinition indexDefinition = null;
      ODocument metadata = null;

      while (jsonReader.lastChar() != '}') {
        final String fieldName = jsonReader.readString(OJSONReader.FIELD_ASSIGNMENT);
        if (fieldName.equals("name"))
          indexName = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);
        else if (fieldName.equals("type"))
          indexType = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);
        else if (fieldName.equals("algorithm"))
          indexAlgorithm = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);
        else if (fieldName.equals("clustersToIndex"))
          clustersToIndex = importClustersToIndex();
        else if (fieldName.equals("definition")) {
          indexDefinition = importIndexDefinition();
          jsonReader.readNext(OJSONReader.NEXT_IN_OBJECT);
        } else if (fieldName.equals("metadata")) {
          String jsonMetadata = jsonReader.readString(OJSONReader.END_OBJECT, true);
          metadata = new ODocument().fromJSON(jsonMetadata);
          jsonReader.readNext(OJSONReader.NEXT_IN_OBJECT);
        } else if (fieldName.equals("blueprintsIndexClass"))
          blueprintsIndexClass = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);
      }

      if (indexName == null)
        throw new IllegalArgumentException("Index name is missing");

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

        OIndex index = indexManager.createIndex(indexName, indexType, indexDefinition, clusterIdsToIndex, null, metadata,
            indexAlgorithm);
        if (blueprintsIndexClass != null) {
          ODocument configuration = index.getConfiguration();
          configuration.field("blueprintsIndexClass", blueprintsIndexClass);
          indexManager.save();
        }

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

    jsonReader.readNext(OJSONReader.NEXT_IN_OBJECT);

    return indexDefinition;
  }

  private void migrateLinksInImportedDocuments() throws IOException {
    listener.onMessage("\n\nStarted migration of links (-migrateLinks=true). Links are going to be updated according to new RIDs:");

    final long begin = System.currentTimeMillis();
    long last = begin;
    long documentsLastLap = 0;

    long totalDocuments = 0;
    Collection<String> clusterNames = database.getClusterNames();
    for (String clusterName : clusterNames) {
      if (OMetadataDefault.CLUSTER_INDEX_NAME.equals(clusterName) || OMetadataDefault.CLUSTER_INTERNAL_NAME.equals(clusterName)
          || OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME.equals(clusterName))
        continue;

      long documents = 0;
      String prefix = "";

      listener.onMessage("\n- Cluster " + clusterName + "...");

      final int clusterId = database.getClusterIdByName(clusterName);
      final long clusterRecords = database.countClusterElements(clusterId);
      OStorage storage = database.getStorage();

      OPhysicalPosition[] positions = storage.ceilingPhysicalPositions(clusterId, new OPhysicalPosition(0));
      while (positions.length > 0) {
        for (OPhysicalPosition position : positions) {
          ORecord record = database.load(new ORecordId(clusterId, position.clusterPosition));
          if (record instanceof ODocument) {
            ODocument document = (ODocument) record;
            rewriteLinksInDocument(document);

            documents++;
            documentsLastLap++;
            totalDocuments++;

            final long now = System.currentTimeMillis();
            if (now - last > IMPORT_RECORD_DUMP_LAP_EVERY_MS) {
              listener.onMessage(String.format("\n--- Migrated %,d of %,d records (%,.2f/sec)", documents, clusterRecords,
                  (float) documentsLastLap * 1000 / (float) IMPORT_RECORD_DUMP_LAP_EVERY_MS));

              // RESET LAP COUNTERS
              last = now;
              documentsLastLap = 0;
              prefix = "\n---";
            }
          }
        }

        positions = storage.higherPhysicalPositions(clusterId, positions[positions.length - 1]);
      }

      listener.onMessage(String.format("%s Completed migration of %,d records in current cluster", prefix, documents));
    }

    listener.onMessage(String.format("\nTotal links updated: %,d", totalDocuments));
  }

  private void rewriteLinksInDocument(ODocument document) {
    rewriteLinksInDocument(document, exportImportHashTable);
    document.save();
  }

  protected static void rewriteLinksInDocument(ODocument document, OIndex<OIdentifiable> rewrite) {
    LinkConverter.INSTANCE.setExportImportHashTable(rewrite);
    final LinksRewriter rewriter = new LinksRewriter();
    final ODocumentFieldWalker documentFieldWalker = new ODocumentFieldWalker();
    documentFieldWalker.walkDocument(document, rewriter);
  }
}
