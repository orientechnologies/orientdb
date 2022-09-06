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
package com.orientechnologies.orient.core.db.tool;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.index.ORuntimeKeyIndexDefinition;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerJSON;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

/**
 * Export data from a database to a file.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class ODatabaseExport extends ODatabaseImpExpAbstract {
  public static final int EXPORTER_VERSION = 13;

  protected OJSONWriter writer;
  protected long recordExported;
  protected int compressionLevel = Deflater.BEST_SPEED;
  protected int compressionBuffer = 16384; // 16Kb

  private final String tempFileName;

  public ODatabaseExport(
      final ODatabaseDocumentInternal iDatabase,
      final String iFileName,
      final OCommandOutputListener iListener)
      throws IOException {
    super(iDatabase, iFileName, iListener);

    if (fileName == null) throw new IllegalArgumentException("file name missing");

    if (!fileName.endsWith(".gz")) {
      fileName += ".gz";
    }
    OFileUtils.prepareForFileCreationOrReplacement(Paths.get(fileName), this, "exporting");

    this.tempFileName = fileName + ".tmp";
    OFileUtils.prepareForFileCreationOrReplacement(Paths.get(tempFileName), this, "exporting");

    final GZIPOutputStream gzipOS =
        new GZIPOutputStream(new FileOutputStream(tempFileName), compressionBuffer) {
          {
            def.setLevel(compressionLevel);
          }
        };

    writer = new OJSONWriter(new OutputStreamWriter(gzipOS));
    writer.beginObject();
  }

  public ODatabaseExport(
      final ODatabaseDocumentInternal iDatabase,
      final OutputStream iOutputStream,
      final OCommandOutputListener iListener)
      throws IOException {
    super(iDatabase, "streaming", iListener);
    this.tempFileName = null;

    writer = new OJSONWriter(new OutputStreamWriter(iOutputStream));
    writer.beginObject();
  }

  @Override
  public void run() {
    exportDatabase();
  }

  @Override
  public ODatabaseExport setOptions(final String s) {
    super.setOptions(s);
    return this;
  }

  public ODatabaseExport exportDatabase() {
    try {
      listener.onMessage(
          "\nStarted export of database '" + database.getName() + "' to " + fileName + "...");

      long time = System.nanoTime();

      if (includeInfo) exportInfo();
      if (includeClusterDefinitions) exportClusters();
      if (includeSchema) exportSchema();
      if (includeRecords) exportRecords();
      if (includeIndexDefinitions) exportIndexDefinitions();
      if (includeManualIndexes) exportManualIndexes();

      listener.onMessage(
          "\n\nDatabase export completed in " + ((System.nanoTime() - time) / 1000000) + "ms");

      writer.flush();
    } catch (Exception e) {
      OLogManager.instance()
          .error(this, "Error on exporting database '%s' to: %s", e, database.getName(), fileName);
      throw new ODatabaseExportException(
          "Error on exporting database '" + database.getName() + "' to: " + fileName, e);
    } finally {
      close();
    }
    return this;
  }

  private long exportRecords() throws IOException {
    long totalFoundRecords = 0;
    long totalExportedRecords = 0;

    int level = 1;
    listener.onMessage("\nExporting records...");

    if (excludeClasses != null && !excludeClasses.isEmpty()) {
      final OSchema schema = database.getMetadata().getSchema();
      for (String cls : excludeClasses) {
        final OClass schemaClass = schema.getClass(cls);
        if (schemaClass == null) {
          listener.onMessage("\nWARN: Can not find class with name " + cls);
        } else {
          final int[] clusterIds = schemaClass.getClusterIds();
          if (clusterIds != null) {
            for (final int clusterId : clusterIds) {
              final String clusterName =
                  database.getClusterNameById(clusterId).toUpperCase(Locale.ENGLISH);
              if (!excludeClusters.contains(clusterName)) {
                listener.onMessage(
                    "\n- Cluster " + clusterName + " will be excluded during the export...");
                excludeClusters.add(clusterName);
              }
            }
          }
        }
      }
    }

    final Set<ORID> brokenRids = new HashSet<>();

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
          if (!includeClusters.contains(clusterName.toUpperCase(Locale.ENGLISH))) continue;
        } else if (excludeClusters != null) {
          if (excludeClusters.contains(clusterName.toUpperCase(Locale.ENGLISH))) continue;
        }

        if (excludeClusters != null
            && excludeClusters.contains(clusterName.toUpperCase(Locale.ENGLISH))) continue;

        clusterExportedRecordsTot = database.countClusterElements(clusterName);
      } else if (includeClusters != null && !includeClusters.isEmpty()) continue;

      listener.onMessage(
          "\n- Cluster "
              + (clusterName != null ? "'" + clusterName + "'" : "NULL")
              + " (id="
              + i
              + ")...");

      long clusterExportedRecordsCurrent = 0;
      if (clusterName != null) {
        ORecord rec = null;
        try {
          ORecordIteratorCluster<ORecord> it = database.browseCluster(clusterName);

          for (; it.hasNext(); ) {

            rec = it.next();
            if (rec instanceof ODocument) {
              // CHECK IF THE CLASS OF THE DOCUMENT IS INCLUDED
              ODocument doc = (ODocument) rec;
              final String className =
                  doc.getClassName() != null
                      ? doc.getClassName().toUpperCase(Locale.ENGLISH)
                      : null;
              if (includeClasses != null) {
                if (!includeClasses.contains(className)) continue;
              } else if (excludeClasses != null) {
                if (excludeClasses.contains(className)) continue;
              }
            } else if (includeClasses != null && !includeClasses.isEmpty()) continue;

            if (exportRecord(
                clusterExportedRecordsTot, clusterExportedRecordsCurrent, rec, brokenRids))
              clusterExportedRecordsCurrent++;
          }

          brokenRids.addAll(it.getBrokenRIDs());
        } catch (OIOException e) {
          OLogManager.instance()
              .error(
                  this,
                  "\nError on exporting record %s because of I/O problems",
                  e,
                  rec == null ? null : rec.getIdentity());
          // RE-THROW THE EXCEPTION UP
          throw e;
        } catch (Exception t) {
          if (rec != null) {
            final byte[] buffer = rec.toStream();

            OLogManager.instance()
                .error(
                    this,
                    "\nError on exporting record %s. It seems corrupted; size: %d bytes, raw content (as string):\n==========\n%s\n==========",
                    t,
                    rec.getIdentity(),
                    buffer.length,
                    new String(buffer));
          }
        }
      }

      listener.onMessage(
          "OK (records=" + clusterExportedRecordsCurrent + "/" + clusterExportedRecordsTot + ")");

      totalExportedRecords += clusterExportedRecordsCurrent;
      totalFoundRecords += clusterExportedRecordsTot;
    }
    writer.endCollection(level, true);

    listener.onMessage(
        "\n\nDone. Exported "
            + totalExportedRecords
            + " of total "
            + totalFoundRecords
            + " records. "
            + brokenRids.size()
            + " records were detected as broken\n");

    writer.beginCollection(level, true, "brokenRids");

    boolean firsBrokenRid = true;
    for (final ORID rid : brokenRids) {
      if (firsBrokenRid) {
        firsBrokenRid = false;
      } else {
        writer.append(",");
      }
      writer.append(rid.toString());
    }
    writer.endCollection(level, true);

    return totalExportedRecords;
  }

  public void close() {
    database.declareIntent(null);

    if (writer == null) return;

    try {
      writer.endObject();
      writer.close();
      writer = null;
    } catch (IOException e) {
      OLogManager.instance()
          .error(this, "Error on exporting database '%s' to: %s", e, database.getName(), fileName);
      throw new ODatabaseExportException(
          "Error on exporting database '" + database.getName() + "' to: " + fileName, e);
    }

    if (tempFileName != null) // may be null if writing to an output stream w/o file
    try {
        OFileUtils.atomicMoveWithFallback(Paths.get(tempFileName), Paths.get(fileName), this);
      } catch (IOException e) {
        OLogManager.instance()
            .error(
                this, "Error on exporting database '%s' to: %s", e, database.getName(), fileName);
        throw new ODatabaseExportException(
            "Error on exporting database '" + database.getName() + "' to: " + fileName, e);
      }
  }

  private int getMaxClusterId() {
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
    else super.parseSetting(option, items);
  }

  private void exportClusters() throws IOException {
    listener.onMessage("\nExporting clusters...");

    writer.beginCollection(1, true, "clusters");
    int exportedClusters = 0;

    int maxClusterId = getMaxClusterId();

    for (int clusterId = 0; clusterId <= maxClusterId; ++clusterId) {

      final String clusterName = database.getClusterNameById(clusterId);

      // exclude removed clusters
      if (clusterName == null) continue;

      // CHECK IF THE CLUSTER IS INCLUDED
      if (includeClusters != null) {
        if (!includeClusters.contains(clusterName.toUpperCase(Locale.ENGLISH))) continue;
      } else if (excludeClusters != null) {
        if (excludeClusters.contains(clusterName.toUpperCase(Locale.ENGLISH))) continue;
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
    writer.writeAttribute(2, true, "exporter-version", EXPORTER_VERSION);
    writer.writeAttribute(2, true, "engine-version", OConstants.getVersion());
    final String engineBuild = OConstants.getBuildNumber();
    if (engineBuild != null) writer.writeAttribute(2, true, "engine-build", engineBuild);
    writer.writeAttribute(2, true, "storage-config-version", OStorageConfiguration.CURRENT_VERSION);
    writer.writeAttribute(2, true, "schema-version", OSchemaShared.CURRENT_VERSION_NUMBER);
    writer.writeAttribute(
        2,
        true,
        "schemaRecordId",
        database.getStorageInfo().getConfiguration().getSchemaRecordId());
    writer.writeAttribute(
        2,
        true,
        "indexMgrRecordId",
        database.getStorageInfo().getConfiguration().getIndexMgrRecordId());
    writer.endObject(1, true);

    listener.onMessage("OK");
  }

  private void exportIndexDefinitions() throws IOException {
    listener.onMessage("\nExporting index info...");
    writer.beginCollection(1, true, "indexes");

    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();
    indexManager.reload();

    final Collection<? extends OIndex> indexes = indexManager.getIndexes(database);

    for (OIndex index : indexes) {
      final String clsName =
          index.getDefinition() != null ? index.getDefinition().getClassName() : null;
      if (ODatabaseImport.EXPORT_IMPORT_CLASS_NAME.equals(clsName)) {
        continue;
      }

      // CHECK TO FILTER CLASS
      if (includeClasses != null) {
        if (!includeClasses.contains(clsName)) continue;
      } else if (excludeClasses != null) {
        if (excludeClasses.contains(clsName)) continue;
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

      final ODocument metadata = index.getMetadata();
      if (metadata != null) writer.writeAttribute(4, true, "metadata", metadata);

      final ODocument configuration = index.getConfiguration();
      if (configuration.field("blueprintsIndexClass") != null)
        writer.writeAttribute(
            4, true, "blueprintsIndexClass", configuration.field("blueprintsIndexClass"));

      writer.endObject(2, true);
      listener.onMessage("OK");
    }

    writer.endCollection(1, true);
    listener.onMessage("\nOK (" + indexes.size() + " indexes)");
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private void exportManualIndexes() throws IOException {
    listener.onMessage("\nExporting manual indexes content...");

    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();
    indexManager.reload();

    final Collection<? extends OIndex> indexes = indexManager.getIndexes(database);

    ODocument exportEntry = new ODocument();

    int manualIndexes = 0;
    for (OIndex index : indexes) {
      if (!index.isAutomatic()) {
        if (manualIndexes == 0) {
          writer.beginCollection(1, true, "manualIndexes");
        }

        listener.onMessage("\n- Exporting index " + index.getName() + " ...");

        writer.beginObject(2, true, null);
        writer.writeAttribute(3, true, "name", index.getName());

        List<ODocument> indexContent =
            database.query(new OSQLSynchQuery<ODocument>("select from index:" + index.getName()));

        writer.beginCollection(3, true, "content");

        int i = 0;
        for (ODocument indexEntry : indexContent) {
          if (i > 0) writer.append(",");

          indexEntry.setLazyLoad(false);
          final OIndexDefinition indexDefinition = index.getDefinition();

          exportEntry.reset();
          exportEntry.setLazyLoad(false);

          if (indexDefinition instanceof ORuntimeKeyIndexDefinition
              && ((ORuntimeKeyIndexDefinition) indexDefinition).getSerializer() != null) {
            final OBinarySerializer binarySerializer =
                ((ORuntimeKeyIndexDefinition) indexDefinition).getSerializer();

            final int dataSize = binarySerializer.getObjectSize(indexEntry.field("key"));
            final byte[] binaryContent = new byte[dataSize];
            binarySerializer.serialize(indexEntry.field("key"), binaryContent, 0);

            exportEntry.field("binary", true);
            exportEntry.field("key", binaryContent);
          } else {
            exportEntry.field("binary", false);
            exportEntry.field("key", indexEntry.<Object>field("key"));
          }
          exportEntry.field("rid", indexEntry.<Object>field("rid"));

          i++;

          writer.append(exportEntry.toJSON());

          final long percent = indexContent.size() / 10;
          if (percent > 0 && (i % percent) == 0) listener.onMessage(".");
        }
        writer.endCollection(3, true);

        writer.endObject(2, true);
        listener.onMessage("OK (entries=" + index.getInternal().size() + ")");
        manualIndexes++;
      }
    }

    if (manualIndexes > 0) {
      writer.endCollection(1, true);
    }
    listener.onMessage("\nOK (" + manualIndexes + " manual indexes)");
  }

  private void exportSchema() throws IOException {
    listener.onMessage("\nExporting schema...");

    writer.beginObject(1, true, "schema");
    final OSchema schema = (database.getMetadata()).getImmutableSchemaSnapshot();
    writer.writeAttribute(2, true, "version", schema.getVersion());
    writer.writeAttribute(2, false, "blob-clusters", database.getBlobClusterIds());
    if (!schema.getClasses().isEmpty()) {
      writer.beginCollection(2, true, "classes");

      final List<OClass> classes = new ArrayList<>(schema.getClasses());
      Collections.sort(classes);

      for (OClass cls : classes) {
        // CHECK TO FILTER CLASS
        if (includeClasses != null) {
          if (!includeClasses.contains(cls.getName().toUpperCase(Locale.ENGLISH))) continue;
        } else if (excludeClasses != null) {
          if (excludeClasses.contains(cls.getName().toUpperCase(Locale.ENGLISH))) continue;
        }

        writer.beginObject(3, true, null);
        writer.writeAttribute(0, false, "name", cls.getName());
        writer.writeAttribute(0, false, "default-cluster-id", cls.getDefaultClusterId());
        writer.writeAttribute(0, false, "cluster-ids", cls.getClusterIds());
        if (cls.getOverSize() > 1)
          writer.writeAttribute(0, false, "oversize", cls.getClassOverSize());
        if (cls.isStrictMode()) writer.writeAttribute(0, false, "strictMode", cls.isStrictMode());
        if (!cls.getSuperClasses().isEmpty())
          writer.writeAttribute(0, false, "super-classes", cls.getSuperClassesNames());
        if (cls.getShortName() != null)
          writer.writeAttribute(0, false, "short-name", cls.getShortName());
        if (cls.isAbstract()) writer.writeAttribute(0, false, "abstract", cls.isAbstract());
        writer.writeAttribute(
            0, false, "cluster-selection", cls.getClusterSelection().getName()); // @SINCE 1.7

        if (!cls.properties().isEmpty()) {
          writer.beginCollection(4, true, "properties");

          final List<OProperty> properties = new ArrayList<OProperty>(cls.declaredProperties());
          Collections.sort(properties);

          for (OProperty p : properties) {
            writer.beginObject(5, true, null);
            writer.writeAttribute(0, false, "name", p.getName());
            writer.writeAttribute(0, false, "type", p.getType().toString());
            if (p.isMandatory()) writer.writeAttribute(0, false, "mandatory", p.isMandatory());
            if (p.isReadonly()) writer.writeAttribute(0, false, "readonly", p.isReadonly());
            if (p.isNotNull()) writer.writeAttribute(0, false, "not-null", p.isNotNull());
            if (p.getLinkedClass() != null)
              writer.writeAttribute(0, false, "linked-class", p.getLinkedClass().getName());
            if (p.getLinkedType() != null)
              writer.writeAttribute(0, false, "linked-type", p.getLinkedType().toString());
            if (p.getMin() != null) writer.writeAttribute(0, false, "min", p.getMin());
            if (p.getMax() != null) writer.writeAttribute(0, false, "max", p.getMax());
            if (p.getCollate() != null)
              writer.writeAttribute(0, false, "collate", p.getCollate().getName());
            if (p.getDefaultValue() != null)
              writer.writeAttribute(0, false, "default-value", p.getDefaultValue());
            if (p.getRegexp() != null) writer.writeAttribute(0, false, "regexp", p.getRegexp());
            final Set<String> customKeys = p.getCustomKeys();
            final Map<String, String> custom = new HashMap<>();
            for (String key : customKeys) custom.put(key, p.getCustom(key));

            if (!custom.isEmpty()) writer.writeAttribute(0, false, "customFields", custom);

            writer.endObject(0, false);
          }
          writer.endCollection(4, true);
        }
        final Set<String> customKeys = cls.getCustomKeys();
        final Map<String, String> custom = new HashMap<String, String>();
        for (String key : customKeys) custom.put(key, cls.getCustom(key));

        if (!custom.isEmpty()) {
          writer.writeAttribute(0, false, "customFields", custom);
        }
        writer.endObject(3, true);
      }
      writer.endCollection(2, true);
    }

    writer.endObject(1, true);

    listener.onMessage("OK (" + schema.getClasses().size() + " classes)");
  }

  private boolean exportRecord(long recordTot, long recordNum, ORecord rec, Set<ORID> brokenRids) {
    if (rec != null)
      try {
        if (rec.getIdentity().isValid()) rec.reload();
        if (useLineFeedForRecords) writer.append("\n");
        if (recordExported > 0) writer.append(",");

        final String format = ORecordAbstract.BASE_FORMAT + ",dateAsLong";
        ORecordSerializerJSON.INSTANCE.toString(rec, writer, format == null ? "" : format, true);

        recordExported++;
        recordNum++;

        if (recordTot > 10 && (recordNum + 1) % (recordTot / 10) == 0) listener.onMessage(".");

        return true;
      } catch (final Exception t) {
        if (rec != null) {
          final ORID rid = rec.getIdentity().copy();

          if (rid != null) {
            brokenRids.add(rid);
          }

          final byte[] buffer = rec.toStream();

          OLogManager.instance()
              .error(
                  this,
                  "\nError on exporting record %s. It seems corrupted; size: %d bytes, raw content (as string):\n==========\n%s\n==========",
                  t,
                  rec.getIdentity(),
                  buffer.length,
                  new String(buffer));
        }
      }

    return false;
  }
}
