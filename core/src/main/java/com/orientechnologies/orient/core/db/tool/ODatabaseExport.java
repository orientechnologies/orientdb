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

import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexManagerProxy;
import com.orientechnologies.orient.core.index.ORuntimeKeyIndexDefinition;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.type.tree.provider.OMVRBTreeMapProvider;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

/**
 * Export data from a database to a file.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ODatabaseExport extends ODatabaseImpExpAbstract {
  public static final int VERSION           = 11;

  protected OJSONWriter   writer;
  protected long          recordExported;
  protected int           compressionLevel  = Deflater.BEST_SPEED;
  protected int           compressionBuffer = 16384;              // 16Kb

  public ODatabaseExport(final ODatabaseDocumentInternal iDatabase, final String iFileName, final OCommandOutputListener iListener)
      throws IOException {
    super(iDatabase, iFileName, iListener);

    if (fileName == null)
      throw new IllegalArgumentException("file name missing");

    if (!fileName.endsWith(".gz")) {
      fileName += ".gz";
    }
    final File f = new File(fileName);
    if (f.getParentFile() != null)
      f.getParentFile().mkdirs();
    if (f.exists())
      f.delete();

    final GZIPOutputStream gzipOS = new GZIPOutputStream(new FileOutputStream(fileName), compressionBuffer) {
      {
        def.setLevel(compressionLevel);
      }
    };

    writer = new OJSONWriter(new OutputStreamWriter(gzipOS));
    writer.beginObject();
  }

  public ODatabaseExport(final ODatabaseDocumentInternal iDatabase, final OutputStream iOutputStream,
      final OCommandOutputListener iListener) throws IOException {
    super(iDatabase, "streaming", iListener);

    writer = new OJSONWriter(new OutputStreamWriter(iOutputStream));
    writer.beginObject();
  }

  @Override
  public ODatabaseExport setOptions(final String s) {
    super.setOptions(s);
    return this;
  }

  public ODatabaseExport exportDatabase() {
    try {
      listener.onMessage("\nStarted export of database '" + database.getName() + "' to " + fileName + "...");

      long time = System.currentTimeMillis();

      if (includeInfo)
        exportInfo();
      if (includeClusterDefinitions)
        exportClusters();
      if (includeSchema)
        exportSchema();
      if (includeRecords)
        exportRecords();
      if (includeIndexDefinitions)
        exportIndexDefinitions();
      if (includeManualIndexes)
        exportManualIndexes();

      listener.onMessage("\n\nDatabase export completed in " + (System.currentTimeMillis() - time) + "ms");

      writer.flush();
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on exporting database '%s' to: %s", e, database.getName(), fileName);
      throw new ODatabaseExportException("Error on exporting database '" + database.getName() + "' to: " + fileName, e);
    } finally {
      close();
    }
    return this;
  }

  public long exportRecords() throws IOException {
    long totalFoundRecords = 0;
    long totalExportedRecords = 0;

    int level = 1;
    listener.onMessage("\nExporting records...");

    writer.beginCollection(level, true, "records");
    int exportedClusters = 0;
    int maxClusterId = getMaxClusterId();
    for (int i = 0; exportedClusters <= maxClusterId; ++i) {
      String clusterName = database.getClusterNameById(i);

      exportedClusters++;

      long clusterExportedRecordsTot = 0;

      if (clusterName != null) {
        // CHECK IF THE CLUSTER IS INCLUDED
        if (includeClusters != null) {
          if (!includeClusters.contains(clusterName.toUpperCase()))
            continue;
        } else if (excludeClusters != null) {
          if (excludeClusters.contains(clusterName.toUpperCase()))
            continue;
        }

        if (excludeClusters != null && excludeClusters.contains(clusterName.toUpperCase()))
          continue;

        clusterExportedRecordsTot = database.countClusterElements(clusterName);
      } else if (includeClusters != null && !includeClusters.isEmpty())
        continue;

      listener.onMessage("\n- Cluster " + (clusterName != null ? "'" + clusterName + "'" : "NULL") + " (id=" + i + ")...");

      long clusterExportedRecordsCurrent = 0;
      if (clusterName != null) {
        ORecord rec = null;
        try {
          for (ORecordIteratorCluster<ORecord> it = database.browseCluster(clusterName); it.hasNext();) {

            rec = it.next();
            if (rec instanceof ODocument) {
              // CHECK IF THE CLASS OF THE DOCUMENT IS INCLUDED
              ODocument doc = (ODocument) rec;
              final String className = doc.getClassName() != null ? doc.getClassName().toUpperCase() : null;
              if (includeClasses != null) {
                if (!includeClasses.contains(className))
                  continue;
              } else if (excludeClasses != null) {
                if (excludeClasses.contains(className))
                  continue;
              }
            } else if (includeClasses != null && !includeClasses.isEmpty())
              continue;

            if (exportRecord(clusterExportedRecordsTot, clusterExportedRecordsCurrent, rec))
              clusterExportedRecordsCurrent++;
          }
        } catch (IOException e) {
          OLogManager.instance().error(this, "\nError on exporting record %s because of I/O problems", e, rec.getIdentity());
          // RE-THROW THE EXCEPTION UP
          throw e;
        } catch (OIOException e) {
          OLogManager.instance().error(this, "\nError on exporting record %s because of I/O problems", e,
              rec == null ? null : rec.getIdentity());
          // RE-THROW THE EXCEPTION UP
          throw e;
        } catch (Throwable t) {
          if (rec != null) {
            final byte[] buffer = rec.toStream();

            OLogManager
                .instance()
                .error(
                    this,
                    "\nError on exporting record %s. It seems corrupted; size: %d bytes, raw content (as string):\n==========\n%s\n==========",
                    t, rec.getIdentity(), buffer.length, new String(buffer));
          }
        }
      }

      listener.onMessage("OK (records=" + clusterExportedRecordsCurrent + "/" + clusterExportedRecordsTot + ")");

      totalExportedRecords += clusterExportedRecordsCurrent;
      totalFoundRecords += clusterExportedRecordsTot;
    }
    writer.endCollection(level, true);

    listener.onMessage("\n\nDone. Exported " + totalExportedRecords + " of total " + totalFoundRecords + " records\n");

    return totalExportedRecords;
  }

  public void close() {
    database.declareIntent(null);

    if (writer == null)
      return;

    try {
      writer.endObject();
      writer.close();
      writer = null;
    } catch (IOException e) {
    }
  }

  protected int getMaxClusterId() {
    int totalCluster = -1;
    for (String clusterName : database.getClusterNames()) {
      if (database.getClusterIdByName(clusterName) > totalCluster)
        totalCluster = database.getClusterIdByName(clusterName);
    }
    return totalCluster;
  }

  @Override
  protected void parseSetting(final String option, final List<String> items) {
    if (option.equalsIgnoreCase("-compressionLevel"))
      compressionLevel = Integer.parseInt(items.get(0));
    else if (option.equalsIgnoreCase("-compressionBuffer"))
      compressionBuffer = Integer.parseInt(items.get(0));
    else
      super.parseSetting(option, items);
  }

  private void exportClusters() throws IOException {
    listener.onMessage("\nExporting clusters...");

    writer.beginCollection(1, true, "clusters");
    int exportedClusters = 0;

    int maxClusterId = getMaxClusterId();

    for (int clusterId = 0; clusterId <= maxClusterId; ++clusterId) {

      final String clusterName = database.getClusterNameById(clusterId);

      // exclude removed clusters
      if (clusterName == null)
        continue;

      // CHECK IF THE CLUSTER IS INCLUDED
      if (includeClusters != null) {
        if (!includeClusters.contains(clusterName.toUpperCase()))
          continue;
      } else if (excludeClusters != null) {
        if (excludeClusters.contains(clusterName.toUpperCase()))
          continue;
      }

      writer.beginObject(2, true, null);

      writer.writeAttribute(0, false, "name", clusterName);
      writer.writeAttribute(0, false, "id", clusterId);

      exportedClusters++;
      writer.endObject(2, false);
    }

    listener.onMessage("OK (" + exportedClusters + " clusters)");

    writer.endCollection(1, true);
  }

  private void exportInfo() throws IOException {
    listener.onMessage("\nExporting database info...");

    writer.beginObject(1, true, "info");
    writer.writeAttribute(2, true, "name", database.getName().replace('\\', '/'));
    writer.writeAttribute(2, true, "default-cluster-id", database.getDefaultClusterId());
    writer.writeAttribute(2, true, "exporter-version", VERSION);
    writer.writeAttribute(2, true, "engine-version", OConstants.ORIENT_VERSION);
    final String engineBuild = OConstants.getBuildNumber();
    if (engineBuild != null)
      writer.writeAttribute(2, true, "engine-build", engineBuild);
    writer.writeAttribute(2, true, "storage-config-version", OStorageConfiguration.CURRENT_VERSION);
    writer.writeAttribute(2, true, "schema-version", OSchemaShared.CURRENT_VERSION_NUMBER);
    writer.writeAttribute(2, true, "mvrbtree-version", OMVRBTreeMapProvider.CURRENT_PROTOCOL_VERSION);
    writer.writeAttribute(2, true, "schemaRecordId", database.getStorage().getConfiguration().schemaRecordId);
    writer.writeAttribute(2, true, "indexMgrRecordId", database.getStorage().getConfiguration().indexMgrRecordId);
    writer.endObject(1, true);

    listener.onMessage("OK");
  }

  private void exportIndexDefinitions() throws IOException {
    listener.onMessage("\nExporting index info...");
    writer.beginCollection(1, true, "indexes");

    final OIndexManagerProxy indexManager = database.getMetadata().getIndexManager();
    indexManager.reload();

    final Collection<? extends OIndex<?>> indexes = indexManager.getIndexes();

    for (OIndex<?> index : indexes) {
      if (index.getName().equals(ODatabaseImport.EXPORT_IMPORT_MAP_NAME))
        continue;

      final String clsName = index.getDefinition() != null ? index.getDefinition().getClassName() : null;

      // CHECK TO FILTER CLASS
      if (includeClasses != null) {
        if (!includeClasses.contains(clsName))
          continue;
      } else if (excludeClasses != null) {
        if (excludeClasses.contains(clsName))
          continue;
      }

      listener.onMessage("\n- Index " + index.getName() + "...");
      writer.beginObject(2, true, null);
      writer.writeAttribute(3, true, "name", index.getName());
      writer.writeAttribute(3, true, "type", index.getType());
      if (index.getAlgorithm() != null)
        writer.writeAttribute(3, true, "algorithm", index.getAlgorithm());

      if (!index.getClusters().isEmpty())
        writer.writeAttribute(3, true, "clustersToIndex", index.getClusters());

      if (index.getDefinition() != null) {
        writer.beginObject(4, true, "definition");

        writer.writeAttribute(5, true, "defClass", index.getDefinition().getClass().getName());
        writer.writeAttribute(5, true, "stream", index.getDefinition().toStream());

        writer.endObject(4, true);
      }

      ODocument metadata = index.getMetadata();
      if (metadata != null)
        writer.writeAttribute(4, true, "metadata", metadata);

      final ODocument configuration = index.getConfiguration();
      if (configuration.field("blueprintsIndexClass") != null)
        writer.writeAttribute(4, true, "blueprintsIndexClass", configuration.field("blueprintsIndexClass"));

      writer.endObject(2, true);
      listener.onMessage("OK");
    }

    writer.endCollection(1, true);
    listener.onMessage("\nOK (" + indexes.size() + " indexes)");
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private void exportManualIndexes() throws IOException {
    listener.onMessage("\nExporting manual indexes content...");

    final OIndexManagerProxy indexManager = database.getMetadata().getIndexManager();
    indexManager.reload();

    final Collection<? extends OIndex<?>> indexes = indexManager.getIndexes();

    ODocument exportEntry = new ODocument();

    int manualIndexes = 0;
    writer.beginCollection(1, true, "manualIndexes");
    for (OIndex<?> index : indexes) {
      if (index.getName().equals(ODatabaseImport.EXPORT_IMPORT_MAP_NAME))
        continue;

      if (!index.isAutomatic()) {
        listener.onMessage("\n- Exporting index " + index.getName() + " ...");

        writer.beginObject(2, true, null);
        writer.writeAttribute(3, true, "name", index.getName());

        List<ODocument> indexContent = database.query(new OSQLSynchQuery<ODocument>("select from index:" + index.getName()));

        writer.beginCollection(3, true, "content");

        int i = 0;
        for (ODocument indexEntry : indexContent) {
          if (i > 0)
            writer.append(",");

          indexEntry.setLazyLoad(false);
          final OIndexDefinition indexDefinition = index.getDefinition();

          exportEntry.reset();
          exportEntry.setLazyLoad(false);

          if (indexDefinition instanceof ORuntimeKeyIndexDefinition
              && ((ORuntimeKeyIndexDefinition) indexDefinition).getSerializer() != null) {
            final OBinarySerializer binarySerializer = ((ORuntimeKeyIndexDefinition) indexDefinition).getSerializer();

            final int dataSize = binarySerializer.getObjectSize(indexEntry.field("key"));
            final byte[] binaryContent = new byte[dataSize];
            binarySerializer.serialize(indexEntry.field("key"), binaryContent, 0);

            exportEntry.field("binary", true);
            exportEntry.field("key", binaryContent);
          } else {
            exportEntry.field("binary", false);
            exportEntry.field("key", indexEntry.field("key"));
          }

          exportEntry.field("rid", indexEntry.field("rid"));

          i++;

          writer.append(exportEntry.toJSON());

          final long percent = indexContent.size() / 10;
          if (percent > 0 && (i % percent) == 0)
            listener.onMessage(".");
        }
        writer.endCollection(3, true);

        writer.endObject(2, true);
        listener.onMessage("OK (entries=" + index.getSize() + ")");
        manualIndexes++;
      }
    }
    writer.endCollection(1, true);
    listener.onMessage("\nOK (" + manualIndexes + " manual indexes)");
  }

  private void exportSchema() throws IOException {
    listener.onMessage("\nExporting schema...");

    writer.beginObject(1, true, "schema");
    OSchema s = ((OMetadataInternal) database.getMetadata()).getImmutableSchemaSnapshot();
    writer.writeAttribute(2, true, "version", s.getVersion());

    if (!s.getClasses().isEmpty()) {
      writer.beginCollection(2, true, "classes");

      final List<OClass> classes = new ArrayList<OClass>(s.getClasses());
      Collections.sort(classes);

      for (OClass cls : classes) {
        // CHECK TO FILTER CLASS
        if (includeClasses != null) {
          if (!includeClasses.contains(cls.getName()))
            continue;
        } else if (excludeClasses != null) {
          if (excludeClasses.contains(cls.getName()))
            continue;
        }

        writer.beginObject(3, true, null);
        writer.writeAttribute(0, false, "name", cls.getName());
        writer.writeAttribute(0, false, "default-cluster-id", cls.getDefaultClusterId());
        writer.writeAttribute(0, false, "cluster-ids", cls.getClusterIds());
        if (cls.getOverSize() > 1)
          writer.writeAttribute(0, false, "oversize", cls.getClassOverSize());
        if (cls.isStrictMode())
          writer.writeAttribute(0, false, "strictMode", cls.isStrictMode());
        if (!cls.getSuperClasses().isEmpty())
          writer.writeAttribute(0, false, "super-classes", cls.getSuperClassesNames());
        if (cls.getShortName() != null)
          writer.writeAttribute(0, false, "short-name", cls.getShortName());
        if (cls.isAbstract())
          writer.writeAttribute(0, false, "abstract", cls.isAbstract());
        writer.writeAttribute(0, false, "cluster-selection", cls.getClusterSelection().getName()); // @SINCE 1.7

        if (!cls.properties().isEmpty()) {
          writer.beginCollection(4, true, "properties");

          final List<OProperty> properties = new ArrayList<OProperty>(cls.declaredProperties());
          Collections.sort(properties);

          for (OProperty p : properties) {
            writer.beginObject(5, true, null);
            writer.writeAttribute(0, false, "name", p.getName());
            writer.writeAttribute(0, false, "type", p.getType().toString());
            if (p.isMandatory())
              writer.writeAttribute(0, false, "mandatory", p.isMandatory());
            if (p.isReadonly())
              writer.writeAttribute(0, false, "readonly", p.isReadonly());
            if (p.isNotNull())
              writer.writeAttribute(0, false, "not-null", p.isNotNull());
            if (p.getLinkedClass() != null)
              writer.writeAttribute(0, false, "linked-class", p.getLinkedClass().getName());
            if (p.getLinkedType() != null)
              writer.writeAttribute(0, false, "linked-type", p.getLinkedType().toString());
            if (p.getMin() != null)
              writer.writeAttribute(0, false, "min", p.getMin());
            if (p.getMax() != null)
              writer.writeAttribute(0, false, "max", p.getMax());
            if (p.getCollate() != null)
              writer.writeAttribute(0, false, "collate", p.getCollate().getName());

            final Set<String> customKeys = p.getCustomKeys();
            final Map<String, String> custom = new HashMap<String, String>();
            for (String key : customKeys)
              custom.put(key, p.getCustom(key));

            if (!custom.isEmpty())
              writer.writeAttribute(0, false, "customFields", custom);

            writer.endObject(0, false);
          }
          writer.endCollection(4, true);
        }
        final Set<String> customKeys = cls.getCustomKeys();
        final Map<String, String> custom = new HashMap<String, String>();
        for (String key : customKeys)
          custom.put(key, cls.getCustom(key));

        if (!custom.isEmpty())
          writer.writeAttribute(0, false, "customFields", custom);

        writer.endObject(3, true);
      }
      writer.endCollection(2, true);
    }

    writer.endObject(1, true);

    listener.onMessage("OK (" + s.getClasses().size() + " classes)");
  }

  private boolean exportRecord(long recordTot, long recordNum, ORecord rec) throws IOException {
    if (rec != null)
      try {
        if (rec.getIdentity().isValid())
          rec.reload();

        if (useLineFeedForRecords)
          writer.append("\n");

        if (recordExported > 0)
          writer.append(",");

        writer.append(rec.toJSON("rid,type,version,class,attribSameRow,keepTypes,alwaysFetchEmbedded,dateAsLong"));

        recordExported++;
        recordNum++;

        if (recordTot > 10 && (recordNum + 1) % (recordTot / 10) == 0)
          listener.onMessage(".");

        return true;
      } catch (Throwable t) {
        if (rec != null) {
          final byte[] buffer = rec.toStream();

          OLogManager
              .instance()
              .error(
                  this,
                  "\nError on exporting record %s. It seems corrupted; size: %d bytes, raw content (as string):\n==========\n%s\n==========",
                  t, rec.getIdentity(), buffer.length, new String(buffer));
        }
      }

    return false;
  }
}
