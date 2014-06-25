/*
 *
 *  * Copyright 2010-2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.orient.etl;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.filter.OSQLPredicate;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

public class OImporterTask {
  private          OImporterData data;
  private          long          lastLap;
  private          int           byteParsed;
  private          long          byteToParse;
  private volatile int           recordsCreated;
  private volatile int           recordsUpdated;
  private volatile int           recordsJoined;
  private volatile int           currentLine;
  private volatile int           recordsImported;
  private volatile int           errors;
  private volatile int           warnings;
  private volatile int           recordsImportedInFile;
  private          long          lastDump;
  private          int           lastRow;
  private Map<OSQLPredicate, AtomicInteger> recordSubTemplate = new HashMap<OSQLPredicate, AtomicInteger>();
  private OCommandContext        context;
  private OImporterCallback      callback;
  private OImporterConfiguration template;

  public OImporterTask(final OImporterCallback iCallback, final OImporterData data) {
    this.callback = iCallback;
    this.data = data;
  }

  @SuppressWarnings("unchecked")
  protected Object join(final ODatabaseDocumentTx db, final ODocument iRecord, OClass iClass, Object fieldValue, final OType fieldType, final String joinTargetField, final String iEdgeClass, final OImporterConfiguration iTemplate) {

    final int pos = joinTargetField.indexOf('.');
    final String[] parts = new String[2];
    parts[0] = joinTargetField.substring(0, pos);
    parts[1] = joinTargetField.substring(pos + 1);

    final OClass targetClass = db.getMetadata().getSchema().getClass(parts[0]);
    if (targetClass == null)
      throw new IllegalArgumentException(String.format("* Found join '%s' but target class not exists", joinTargetField));
    final OProperty targetProperty = targetClass.getProperty(parts[1]);
    if (targetProperty != null && !targetProperty.getAllIndexes().isEmpty()) {
      final OIndex<?> idx = targetProperty.getAllIndexes().iterator().next();
      if (idx.getType().equals("UNIQUE")) {
        fieldValue = OType.convert(fieldValue, idx.getKeyTypes()[0].getDefaultJavaType());
        final Object res = ((OIndex<OIdentifiable>) idx).get(fieldValue);

        if (res == null)
          iTemplate.getImplementation().onJoinNotFound(db, context, idx, fieldValue);

        return res;
      }
    }

    final String q = "select from " + parts[0] + " where " + parts[1] + " = '" + fieldValue + "'";
    final List<OIdentifiable> result = db.query(new OSQLSynchQuery<Object>(q));
    if (result == null || result.isEmpty()) {
      // NO LINK
      OLogManager.instance().warn(this, "     + line %d: join record not found from query '%s'", currentLine, q);
      warnings++;
      // return null;
    }

    OClass edgeClass;
    // if (iEdgeClass != null) {
    // edgeClass = db.getEdgeType(iEdgeClass);
    // if (edgeClass == null)
    // // CREATE IT
    // edgeClass = db.createEdgeType(iEdgeClass);
    // } else
    edgeClass = null;

    if (edgeClass != null) {
      // CREATE EDGE
      // fieldValue = db.createEdge(iRecord.getIdentity(), result.get(0).getIdentity());
    } else if (fieldType == OType.LINK) {
      // SINGLE LINK
      if (result.size()>1)
        throw new IllegalArgumentException(String.format(
            "* Found join '%s' against multiple records (%d) using key = %s, while it's configured for only one", joinTargetField,
            result.size(), fieldValue));

      fieldValue = result.get(0).getIdentity();
    } else if (fieldType == OType.LINKSET)
      fieldValue = result;
    else if (fieldType == OType.LINKLIST)
      fieldValue = result;

    return fieldValue;
  }

  protected OClass checkSchema(ODatabaseDocumentTx db, OImporterConfiguration template) {
    OClass cls = checkSchemaForTemplate(db, template);

    if (template.getSubTemplates() != null && !template.getSubTemplates().isEmpty()) {
      for (OImporterConfiguration t : template.getSubTemplates().values()) {
        if (t.getClassName() != null)
          cls = checkSchemaForTemplate(db, t);
      }
    }

    return cls;
  }

  protected OClass checkSchemaForTemplate(ODatabaseDocumentTx db, OImporterConfiguration template) {
    // CREATE THE CLASS IF NOT EXISTS
    OClass cls = db.getMetadata().getSchema().getClass(template.getClassName());
    OClass superCls = null;
    if (template.getSuperClass() != null) {
      // USE THE SUPER CLASS
      superCls = db.getMetadata().getSchema().getClass(template.getSuperClass());
      if (superCls == null)
        superCls = db.getMetadata().getSchema().createClass(template.getSuperClass());
    }

    if (cls == null) {
      if (superCls != null)
        // USE THE SUPER CLASS
        cls = db.getMetadata().getSchema().createClass(template.getClassName(), superCls);
      else
        // NO SUPER CLASS
        cls = db.getMetadata().getSchema().createClass(template.getClassName());
    } else {
      if (superCls != null) {
        if (template.getSuperClass() != null && !superCls.getName().equalsIgnoreCase(template.getSuperClass())
            || cls.getSuperClass() != superCls)
          // CREATE THE DEPENDENCY
          cls.setSuperClass(superCls);
      } else if (cls.getSuperClass() != null)
        // REMOVE IT
        cls.setSuperClass(null);

    }

    // if (template.getDataSegment() != null) {
    // try {
    // db.getStorage().getDataSegmentIdByName(template.getDataSegment());
    // } catch (Exception e) {
    // db.getStorage().addDataSegment(template.getDataSegment());
    // }
    //
    // for (int clusterId : cls.getClusterIds()) {
    // try {
    // db.getStorage().getClusterById(clusterId)
    // .set(com.orientechnologies.orient.core.storage.OCluster.ATTRIBUTES.DATASEGMENT, template.getDataSegment());
    // } catch (IOException e) {
    // throw new OConfigurationException("Cannot assign datasegment '" + template.getDataSegment() + "' to cluster " + clusterId);
    // }
    // }
    // }

    for (Entry<String[], String[]> it : template.getClassIndexes()) {
      final String indexName = it.getKey()[0];
      final String indexType = it.getKey()[1];
      final String[] indexFields = it.getValue();

      OIndex<?> idx = db.getMetadata().getIndexManager().getIndex(indexName);

      if (idx == null) {
        OLogManager.instance().info(this, "Creating new index '%s' of type '%s' against class '%s' fields '%s'...", indexName,
            indexType, template.getClassName(), Arrays.toString(indexFields));

        for (String field : indexFields) {
          OProperty prop = cls.getProperty(field);
          if (prop == null)
            prop = cls.createProperty(field, template.getFieldType(field));
        }
        idx = cls.createIndex(indexName, indexType, indexFields);
        OLogManager.instance().info(this, "Indexed %d records for index '%s'", idx.getSize(), template.getClassName(), it.getKey());
      }
    }

    // CHECK FOR FIELD INDEXES
    for (String field : template.getFieldNames()) {
      final List<OIndex<?>> idxs = new ArrayList<OIndex<?>>(cls.getInvolvedIndexes(field));

      final String idxType = template.getFieldIndex(field);
      if (idxType != null) {
        OProperty prop = cls.getProperty(field);
        if (prop == null)
          prop = cls.createProperty(field, template.getFieldType(field));

        if (idxs == null || idxs.isEmpty()) {
          OLogManager.instance().info(this, "Creating new index of type '%s' against field '%s.%s'...", idxType,
              template.getClassName(), field);

          final OIndex<?> idx = prop.createIndex(idxType.toUpperCase());

          OLogManager.instance().info(this, "Indexed %d records for index '%s.%s'", idx.getSize(), template.getClassName(), field);
        }
      } else {
        // CHECK IF MUST BE REMOVED
        for (OIndex<?> idx : idxs) {
          if (idx.isAutomatic() && idx.getKeyTypes().length == 1) {
            OLogManager.instance().info(this, "Dropping index '%s' against field '%s.%s'...", idx.getName(),
                template.getClassName(), field);
            idx.delete();
          }
        }
      }
    }
    return cls;
  }

  protected OSQLSynchQuery<ODocument> getLookupQuery(final OImporterConfiguration template) {
    final OPair<String, String> key = template.getKey();
    return key != null && key.getValue().equals("overwrite") ? new OSQLSynchQuery<ODocument>("select from "
        + template.getClassName() + " where " + key.getKey() + " = ?") : null;
  }

  private boolean importFile(final ODatabaseDocumentTx db, final File f) {
    errors = 0;
    warnings = 0;
    recordsImportedInFile = 0;
    recordsCreated = 0;
    recordsUpdated = 0;
    recordsJoined = 0;
    byteParsed = 0;
    byteToParse = f.length();

    final long startTime = System.currentTimeMillis();

    try {
      // GET THE TEMPLATE
      String fileName = f.getName();
      if (f.getName().indexOf("-") > -1)
        fileName = fileName.substring(0, f.getName().indexOf("-"));
      else if (f.getName().endsWith(".gz"))
        fileName = fileName.substring(0, f.getName().length() - ".gz".length());

      if (fileName.endsWith(data.importFileExtension))
        fileName = fileName.substring(0, fileName.length() - data.importFileExtension.length());

      // FILE NAME WITHOUT PREFIX
      final int extPos = f.getName().indexOf('.');
      final String finalFileName = extPos > -1 ? f.getName().substring(0, extPos) : f.getName();
      final String finalFileExtension = extPos > -1 ? f.getName().substring(extPos) : "";

      if (template == null)
        return false;

      final OClass cls = checkSchema(db, template);

      final OSQLSynchQuery<ODocument> lookupQuery = getLookupQuery(template);

      final RandomAccessFile raf = new RandomAccessFile(f, "rw");
      try {
        final FileChannel channel = raf.getChannel();
        final java.nio.channels.FileLock lock = channel.tryLock();
        try {

          if (lock == null) {
            // FILE LOCKED, SKIP IT
            OLogManager.instance().debug(this, "File locked, retry it later...", f);
            return false;
          }

          InputStreamReader fileReader = null;
          final FileInputStream fis = new FileInputStream(f);
          try {

            if (f.getName().endsWith(".gz"))
              fileReader = new InputStreamReader(new GZIPInputStream(fis));
            else
              fileReader = new FileReader(f);

            final BufferedReader br = new BufferedReader(fileReader);
            try {
              String thisLine;
              currentLine = -1;
              lastLap = 0;

              context = new OBasicCommandContext();
              context.setVariable("fileName", f.getName());
              context.setVariable("errors", 0);
              context.setVariable("warnings", 0);

              template.getImplementation().onBeforeFile(db, context);
              if (Boolean.TRUE.equals(context.getVariable("skip"))) {
                OLogManager.instance().info(this, "Skipped file %s", f.getName());
                return false;
              }

              while ((thisLine = br.readLine()) != null) {
                byteParsed += thisLine.length() + 1;
                importLine(db, template, cls, lookupQuery, thisLine);
              }

              // DISPLAY LAST LAP PROGRESS
              dumpProgress(db, template);

              template.getImplementation().onAfterFile(db, context);
              context = null;

            } finally {
              br.close();
            }

          } finally {
            fis.close();
            if (fileReader != null)
              fileReader.close();
          }

        } finally {
          if (lock != null)
            lock.release();
          channel.close();
        }
      } finally {
        raf.close();
      }

      callback.onAfterProcessingFile(f);

    } catch (Throwable e) {
      OLogManager.instance().error(this, "Error on importing file %s (imported rows: %d)", e, f, recordsImportedInFile);
      errors++;
    }

    return true;
  }

  @SuppressWarnings("unchecked")
  private void importLine(final ODatabaseDocumentTx db, final OImporterConfiguration template, final OClass cls,
      final OSQLSynchQuery<ODocument> lookupQuery, final String thisLine) {
    currentLine++;

    final long now = System.currentTimeMillis();
    if (now - lastDump >= data.dumpProgressEvery) {
      // DISPLAY LAP PROGRESS
      dumpProgress(db, template);
      lastDump = now;
    }

    ODocument record = new ODocument(template.getClassName());

    context.setVariable("currentRecord", record);
    context.setVariable("currentLine", currentLine);
    context.setVariable("warnings", warnings);
    context.setVariable("errors", errors);

    try {
      final List<String> fields = OStringSerializerHelper.smartSplit(thisLine, new char[] { data.separator.charAt(0) }, 0, -1,
          false, false, false, false);

      if (!template.hasSubTemplates() && fields.size() < template.getMandatoryFields()) {
        OLogManager.instance().info(this,
            "* Error on row %d: found %d fields in row but template has %d mandatory fields configured. Skipped it. Row:\n%s",
            currentLine, fields.size(), template.getMandatoryFields(), thisLine);
        errors++;
        return;
      }

      final Iterator<String> fieldValuesIterator = fields.iterator();
      parseFields(db, template, cls, record, template.getFieldNames().iterator(), fieldValuesIterator);

      for (Entry<OSQLPredicate, OImporterConfiguration> sub : template.getSubTemplates().entrySet()) {
        final Object res = sub.getKey().evaluate(record, null, null);
        if (res != null && res instanceof Boolean && ((Boolean) res)) {
          final OImporterConfiguration subTemplate = sub.getValue();
          parseFields(db, subTemplate, cls, record, subTemplate.getFieldNames().iterator(), fieldValuesIterator);

          // UPDATE STATS
          AtomicInteger current = recordSubTemplate.get(sub.getKey());
          if (current == null) {
            current = new AtomicInteger();
            recordSubTemplate.put(sub.getKey(), current);
          } else
            current.incrementAndGet();
        }
      }

      recordsCreated++;

      if (lookupQuery != null) {
        // SEARCH FOR EXISTENT ENTRY IF ANY
        List<OIdentifiable> existent = db.query(lookupQuery, record.field(template.getKey().getKey()));
        if (existent != null && !existent.isEmpty()) {
          // MERGE IT
          final ODocument current = existent.get(0).getRecord();

          for (String fieldName : record.fieldNames()) {
            Object fieldValue = record.field(fieldName);

            final OSQLPredicate condition = template.getOverwriteIf().get(fieldName);
            if (condition != null) {
              final Object result = condition.evaluate(current, null, context);
              if (result != null && result instanceof Boolean && !((Boolean) result))
                // SKIPT IT
                continue;
            }

            final Object oldValue = current.field(fieldName);
            if (oldValue != null) {
              if (oldValue instanceof Collection<?>) {
                if (fieldValue instanceof Collection<?>)
                  if (!OMultiValue.equals((Collection<Object>) oldValue, (Collection<Object>) fieldValue))
                    ((Collection<Object>) oldValue).addAll((Collection<? extends Object>) fieldValue);
                  else
                    continue;
                else {
                  ((Collection<Object>) oldValue).add(fieldValue);
                  fieldValue = oldValue;
                }
              }
            }

            // OVERWRITE IT
            current.field(fieldName, fieldValue);
          }

          record = current;
          context.setVariable("currentRecord", record);

          recordsCreated--;

          recordsUpdated++;
        }
      }

      template.getImplementation().onBeforeLine(db, context);

      record.unpin();

      if (template.isSave())
        try {
          record.save();
        } catch (Exception e) {
          OLogManager.instance().error(this, "Error on saving record: " + e);
          throw e;
        }

      recordsImportedInFile++;
      recordsImported++;
      warnings = (Integer) context.getVariable("warnings");
      errors = (Integer) context.getVariable("errors");

      template.getImplementation().onAfterLine(db, context);
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on importing line, continue with the rest of the file", e);
      errors++;
    }
  }

  private void parseFields(final ODatabaseDocumentTx db, final OImporterConfiguration template, final OClass cls,
      final ODocument record, Iterator<String> iFieldNames, final Iterator<String> iFieldValues) {

    if (template.getClassName() != null && !record.getClassName().equals(template.getClassName()))
      record.setClassName(template.getClassName());

    while (iFieldNames.hasNext() && iFieldValues.hasNext()) {
      final String fieldName = iFieldNames.next();
      Object fieldValue = iFieldValues.next();

      final OType fieldType = template.getFieldType(fieldName);

      if (fieldType == null)
        continue;

      if (fieldValue != null) {
        if (template.getTrims().contains(fieldName))
          // TRIM IT
          fieldValue = fieldValue.toString().trim();

        fieldValue = OStringSerializerHelper.getStringContent(fieldValue);
      }

      try {
        if (fieldValue != null) {
          if (fieldValue.toString().length() == 0 || fieldValue.toString().equals(data.nullValue))
            fieldValue = null;
          else {
            final String joinTargetField = template.getFieldTargetJoin(fieldName);
            final String edgeClass = template.getFieldEdge(fieldName);

            if (joinTargetField != null) {
              fieldValue = join(db, record, cls, fieldValue, fieldType, joinTargetField, edgeClass, template);
              recordsJoined++;
            } else if (fieldType == OType.DATE || fieldType == OType.DATETIME) {
              final SimpleDateFormat df = new SimpleDateFormat(template.getFieldFormatDetail(fieldName));
              df.setTimeZone(TimeZone.getTimeZone("UTC"));
              fieldValue = df.parse((String) fieldValue);
            } else {
              fieldValue = OType.convert(fieldValue, fieldType.getDefaultJavaType());
            }
          }
        }

        record.field(fieldName, fieldValue);
      } catch (Exception e) {
        OLogManager.instance().info(this, "* Error on row %d field %s", e, currentLine, fieldName);
        continue;
      }
    }
  }

  private void dumpProgress(ODatabaseDocumentTx db, OImporterConfiguration template) {
    final long now = System.currentTimeMillis();
    final float rowSec = (currentLine - lastRow) * 1000f / (now - lastLap);

    OLogManager.instance().info(this,
        "   + %3.2f%% (%d/%d): scanned so far %d rows (%d created, %d updated, %d joins, %d warnings, %d errors). %.2f rows/sec",
        ((double) byteParsed * 100f / (double) byteToParse), byteParsed, byteToParse, currentLine + 1, recordsCreated,
        recordsUpdated, recordsJoined, warnings, errors, rowSec);

    template.getImplementation().onDump(db, context);

    if (!recordSubTemplate.isEmpty()) {
      final StringBuilder subTemplFormat = new StringBuilder();
      for (Entry<OSQLPredicate, AtomicInteger> e : recordSubTemplate.entrySet()) {
        if (subTemplFormat.length() > 0)
          subTemplFormat.append(',');
        subTemplFormat.append("(" + e.getKey().parserText + ")=" + e.getValue().intValue());
      }

      OLogManager.instance().info(this, "   ++ " + subTemplFormat);
    }

    lastLap = now;
    lastRow = currentLine;
  }
}
