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

package com.orientechnologies.orient.etl.transform;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

public class OConfigurableTransformer extends OAbstractTransformer {
  protected ODatabaseDocumentTx db;
  protected long                progress = 0;
  protected volatile int        recordsCreated;
  protected volatile int        recordsUpdated;
  protected volatile int        recordsJoined;
  protected volatile int        recordsImported;
  protected OCommandContext     context;

  public OConfigurableTransformer() {
  }

  @Override
  public void prepare(final ODatabaseDocumentTx db) {
    this.db = db;

    // if (iConfiguration.containsField("separator"))
    // separator = iConfiguration.field("separator");
    // if (iConfiguration.containsField("columnsOnFirstLine"))
    // columnsOnFirstLine = iConfiguration.field("columnsOnFirstLine");
    // if (iConfiguration.containsField("columns"))
    // columns = iConfiguration.field("columns");
  }

  @Override
  public String getName() {
    return "--";
  }

  @Override
  public Object transform(Object input, OCommandContext iContext) {
    return input;
  }

  // @SuppressWarnings("unchecked")
  // protected Object join(final ODatabaseDocumentTx db, final ODocument iRecord, Object fieldValue,
  // final OTransformerConfiguration iConfiguration) {
  //
  // final int pos = joinTargetField.indexOf('.');
  // final String[] parts = new String[2];
  // parts[0] = joinTargetField.substring(0, pos);
  // parts[1] = joinTargetField.substring(pos + 1);
  //
  // final OClass targetClass = db.getMetadata().getSchema().getClass(parts[0]);
  // if (targetClass == null)
  // throw new IllegalArgumentException(String.format("* Found join '%s' but target class not exists", joinTargetField));
  //
  // // LOOK FOR AN INDEX TO USE DIRECTLY (NO SQL PARSING HERE)
  // final OProperty targetProperty = targetClass.getProperty(parts[1]);
  // if (targetProperty != null && !targetProperty.getAllIndexes().isEmpty()) {
  // final OIndex<?> idx = targetProperty.getAllIndexes().iterator().next();
  // if (idx.getType().equals("UNIQUE")) {
  // fieldValue = OType.convert(fieldValue, idx.getKeyTypes()[0].getDefaultJavaType());
  // final Object res = ((OIndex<OIdentifiable>) idx).get(fieldValue);
  //
  // if (res == null)
  // iTemplate.getImplementation().onJoinNotFound(db, context, idx, fieldValue);
  //
  // return res;
  // }
  // }
  //
  // // NO INDEX FOUND, EXECUTE QUERY (COULD BE SLOW)
  // final String q = "select from " + parts[0] + " where " + parts[1] + " = '" + fieldValue + "'";
  // final List<OIdentifiable> result = db.query(new OSQLSynchQuery<Object>(q));
  // if (result == null || result.isEmpty()) {
  // // NO LINK
  // iTemplate.getImplementation().onJoinNotFound(db, context, null, fieldValue);
  // warnings++;
  // }
  //
  // OClass edgeClass;
  // // if (iEdgeClass != null) {
  // // edgeClass = db.getEdgeType(iEdgeClass);
  // // if (edgeClass == null)
  // // // CREATE IT
  // // edgeClass = db.createEdgeType(iEdgeClass);
  // // } else
  // edgeClass = null;
  //
  // if (edgeClass != null) {
  // // CREATE EDGE
  // // fieldValue = db.createEdge(iRecord.getIdentity(), result.get(0).getIdentity());
  // } else if (fieldType == OType.LINK) {
  // // SINGLE LINK
  // if (result.size() > 1)
  // throw new IllegalArgumentException(String.format(
  // "* Found join '%s' against multiple records (%d) using key = %s, while it's configured for only one", joinTargetField,
  // result.size(), fieldValue));
  //
  // fieldValue = result.get(0).getIdentity();
  // } else if (fieldType == OType.LINKSET)
  // fieldValue = result;
  // else if (fieldType == OType.LINKLIST)
  // fieldValue = result;
  //
  // return fieldValue;
  // }
  //
  // protected OClass checkSchema(ODatabaseDocumentTx db, OTransformerConfiguration iConfiguration) {
  // OClass cls = checkSchemaForTemplate(db, iConfiguration);
  //
  // if (iConfiguration.getSubTemplates() != null && !iConfiguration.getSubTemplates().isEmpty()) {
  // for (OTransformerConfiguration t : iConfiguration.getSubTemplates().values()) {
  // if (t.getClassName() != null)
  // cls = checkSchemaForTemplate(db, t);
  // }
  // }
  //
  // return cls;
  // }
  //
  // protected OClass checkSchemaForTemplate(ODatabaseDocumentTx db, OTransformerConfiguration iConfiguration) {
  // // CREATE THE CLASS IF NOT EXISTS
  // OClass cls = db.getMetadata().getSchema().getClass(iConfiguration.getClassName());
  // OClass superCls = null;
  // if (iConfiguration.getSuperClass() != null) {
  // // USE THE SUPER CLASS
  // superCls = db.getMetadata().getSchema().getClass(iConfiguration.getSuperClass());
  // if (superCls == null)
  // superCls = db.getMetadata().getSchema().createClass(iConfiguration.getSuperClass());
  // }
  //
  // if (cls == null) {
  // if (superCls != null)
  // // USE THE SUPER CLASS
  // cls = db.getMetadata().getSchema().createClass(iConfiguration.getClassName(), superCls);
  // else
  // // NO SUPER CLASS
  // cls = db.getMetadata().getSchema().createClass(iConfiguration.getClassName());
  // } else {
  // if (superCls != null) {
  // if (iConfiguration.getSuperClass() != null && !superCls.getName().equalsIgnoreCase(iConfiguration.getSuperClass())
  // || cls.getSuperClass() != superCls)
  // // CREATE THE DEPENDENCY
  // cls.setSuperClass(superCls);
  // } else if (cls.getSuperClass() != null)
  // // REMOVE IT
  // cls.setSuperClass(null);
  //
  // }
  //
  // // if (iConfiguration.getDataSegment() != null) {
  // // try {
  // // db.getStorage().getDataSegmentIdByName(iConfiguration.getDataSegment());
  // // } catch (Exception e) {
  // // db.getStorage().addDataSegment(iConfiguration.getDataSegment());
  // // }
  // //
  // // for (int clusterId : cls.getClusterIds()) {
  // // try {
  // // db.getStorage().getClusterById(clusterId)
  // // .set(com.orientechnologies.orient.core.storage.OCluster.ATTRIBUTES.DATASEGMENT, iConfiguration.getDataSegment());
  // // } catch (IOException e) {
  // // throw new OConfigurationException("Cannot assign datasegment '" + iConfiguration.getDataSegment() + "' to cluster " +
  // // clusterId);
  // // }
  // // }
  // // }
  //
  // for (Entry<String[], String[]> it : iConfiguration.getClassIndexes()) {
  // final String indexName = it.getKey()[0];
  // final String indexType = it.getKey()[1];
  // final String[] indexFields = it.getValue();
  //
  // OIndex<?> idx = db.getMetadata().getIndexManager().getIndex(indexName);
  //
  // if (idx == null) {
  // OLogManager.instance().info(this, "Creating new index '%s' of type '%s' against class '%s' fields '%s'...", indexName,
  // indexType, iConfiguration.getClassName(), Arrays.toString(indexFields));
  //
  // for (String field : indexFields) {
  // OProperty prop = cls.getProperty(field);
  // if (prop == null)
  // prop = cls.createProperty(field, iConfiguration.getFieldType(field));
  // }
  // idx = cls.createIndex(indexName, indexType, indexFields);
  // OLogManager.instance().info(this, "Indexed %d records for index '%s'", idx.getSize(), iConfiguration.getClassName(),
  // it.getKey());
  // }
  // }
  //
  // // CHECK FOR FIELD INDEXES
  // for (String field : iConfiguration.getFieldNames()) {
  // final List<OIndex<?>> idxs = new ArrayList<OIndex<?>>(cls.getInvolvedIndexes(field));
  //
  // final String idxType = iConfiguration.getFieldIndex(field);
  // if (idxType != null) {
  // OProperty prop = cls.getProperty(field);
  // if (prop == null)
  // prop = cls.createProperty(field, iConfiguration.getFieldType(field));
  //
  // if (idxs == null || idxs.isEmpty()) {
  // OLogManager.instance().info(this, "Creating new index of type '%s' against field '%s.%s'...", idxType,
  // iConfiguration.getClassName(), field);
  //
  // final OIndex<?> idx = prop.createIndex(idxType.toUpperCase());
  //
  // OLogManager.instance().info(this, "Indexed %d records for index '%s.%s'", idx.getSize(), iConfiguration.getClassName(),
  // field);
  // }
  // } else {
  // // CHECK IF MUST BE REMOVED
  // for (OIndex<?> idx : idxs) {
  // if (idx.isAutomatic() && idx.getKeyTypes().length == 1) {
  // OLogManager.instance().info(this, "Dropping index '%s' against field '%s.%s'...", idx.getName(),
  // iConfiguration.getClassName(), field);
  // idx.delete();
  // }
  // }
  // }
  // }
  // return cls;
  // }
  //
  // protected OSQLSynchQuery<ODocument> getLookupQuery(final OTransformerConfiguration template) {
  // final OPair<String, String> key = template.getKey();
  // return key != null && key.getValue().equals("overwrite") ? new OSQLSynchQuery<ODocument>("select from "
  // + template.getClassName() + " where " + key.getKey() + " = ?") : null;
  // }
  //
  // private boolean importFile(final ODatabaseDocumentTx db, final File f) {
  // errors = 0;
  // warnings = 0;
  // recordsImportedInFile = 0;
  // recordsCreated = 0;
  // recordsUpdated = 0;
  // recordsJoined = 0;
  // byteParsed = 0;
  // byteToParse = f.length();
  //
  // final long startTime = System.currentTimeMillis();
  //
  // try {
  // // GET THE TEMPLATE
  // String fileName = f.getName();
  // if (f.getName().indexOf("-") > -1)
  // fileName = fileName.substring(0, f.getName().indexOf("-"));
  // else if (f.getName().endsWith(".gz"))
  // fileName = fileName.substring(0, f.getName().length() - ".gz".length());
  //
  // if (fileName.endsWith(data.importFileExtension))
  // fileName = fileName.substring(0, fileName.length() - data.importFileExtension.length());
  //
  // // FILE NAME WITHOUT PREFIX
  // final int extPos = f.getName().indexOf('.');
  // final String finalFileName = extPos > -1 ? f.getName().substring(0, extPos) : f.getName();
  // final String finalFileExtension = extPos > -1 ? f.getName().substring(extPos) : "";
  //
  // if (template == null)
  // return false;
  //
  // final OClass cls = checkSchema(db, template);
  //
  // final OSQLSynchQuery<ODocument> lookupQuery = getLookupQuery(template);
  //
  // final RandomAccessFile raf = new RandomAccessFile(f, "rw");
  // try {
  // final FileChannel channel = raf.getChannel();
  // final java.nio.channels.FileLock lock = channel.tryLock();
  // try {
  //
  // if (lock == null) {
  // // FILE LOCKED, SKIP IT
  // OLogManager.instance().debug(this, "File locked, retry it later...", f);
  // return false;
  // }
  //
  // InputStreamReader fileReader = null;
  // final FileInputStream fis = new FileInputStream(f);
  // try {
  //
  // if (f.getName().endsWith(".gz"))
  // fileReader = new InputStreamReader(new GZIPInputStream(fis));
  // else
  // fileReader = new FileReader(f);
  //
  // final BufferedReader br = new BufferedReader(fileReader);
  // try {
  // String thisLine;
  // currentLine = -1;
  // lastLap = 0;
  //
  // context = new OBasicCommandContext();
  // context.setVariable("fileName", f.getName());
  // context.setVariable("errors", 0);
  // context.setVariable("warnings", 0);
  //
  // template.getImplementation().onBeforeFile(db, context);
  // if (Boolean.TRUE.equals(context.getVariable("skip"))) {
  // OLogManager.instance().info(this, "Skipped file %s", f.getName());
  // return false;
  // }
  //
  // while ((thisLine = br.readLine()) != null) {
  // byteParsed += thisLine.length() + 1;
  // importLine(db, template, cls, lookupQuery, thisLine);
  // }
  //
  // // DISPLAY LAST LAP PROGRESS
  // dumpProgress(db, template);
  //
  // template.getImplementation().onAfterFile(db, context);
  // context = null;
  //
  // } finally {
  // br.close();
  // }
  //
  // } finally {
  // fis.close();
  // if (fileReader != null)
  // fileReader.close();
  // }
  //
  // } finally {
  // if (lock != null)
  // lock.release();
  // channel.close();
  // }
  // } finally {
  // raf.close();
  // }
  //
  // callback.onAfterProcessingFile(f);
  //
  // } catch (Throwable e) {
  // OLogManager.instance().error(this, "Error on importing file %s (imported rows: %d)", e, f, recordsImportedInFile);
  // errors++;
  // }
  //
  // return true;
  // }
  //
  // @SuppressWarnings("unchecked")
  // private void importLine(final ODatabaseDocumentTx db, final OTransformerConfiguration iConfiguration, final OClass cls,
  // final OSQLSynchQuery<ODocument> lookupQuery, final String thisLine) {
  // currentLine++;
  //
  // final long now = System.currentTimeMillis();
  // if (now - lastDump >= data.dumpProgressEvery) {
  // // DISPLAY LAP PROGRESS
  // dumpProgress(db, iConfiguration);
  // lastDump = now;
  // }
  //
  // ODocument record = new ODocument(iConfiguration.getClassName());
  //
  // context.setVariable("currentRecord", record);
  // context.setVariable("currentLine", currentLine);
  // context.setVariable("warnings", warnings);
  // context.setVariable("errors", errors);
  //
  // try {
  // final List<String> fields = OStringSerializerHelper.smartSplit(thisLine, new char[] { data.separator.charAt(0) }, 0, -1,
  // false, false, false, false);
  //
  // if (!iConfiguration.hasSubTemplates() && fields.size() < iConfiguration.getMandatoryFields()) {
  // OLogManager
  // .instance()
  // .info(
  // this,
  // "* Error on row %d: found %d fields in row but iConfiguration has %d mandatory fields configured. Skipped it. Row:\n%s",
  // currentLine, fields.size(), iConfiguration.getMandatoryFields(), thisLine);
  // errors++;
  // return;
  // }
  //
  // final Iterator<String> fieldValuesIterator = fields.iterator();
  // parseFields(db, iConfiguration, cls, record, iConfiguration.getFieldNames().iterator(), fieldValuesIterator);
  //
  // for (Entry<OSQLPredicate, OTransformerConfiguration> sub : iConfiguration.getSubTemplates().entrySet()) {
  // final Object res = sub.getKey().evaluate(record, null, null);
  // if (res != null && res instanceof Boolean && ((Boolean) res)) {
  // final OTransformerConfiguration subTemplate = sub.getValue();
  // parseFields(db, subTemplate, cls, record, subTemplate.getFieldNames().iterator(), fieldValuesIterator);
  //
  // // UPDATE STATS
  // AtomicInteger current = recordSubTemplate.get(sub.getKey());
  // if (current == null) {
  // current = new AtomicInteger();
  // recordSubTemplate.put(sub.getKey(), current);
  // } else
  // current.incrementAndGet();
  // }
  // }
  //
  // recordsCreated++;
  //
  // if (lookupQuery != null) {
  // // SEARCH FOR EXISTENT ENTRY IF ANY
  // List<OIdentifiable> existent = db.query(lookupQuery, record.field(iConfiguration.getKey().getKey()));
  // if (existent != null && !existent.isEmpty()) {
  // // MERGE IT
  // final ODocument current = existent.get(0).getRecord();
  //
  // for (String fieldName : record.fieldNames()) {
  // Object fieldValue = record.field(fieldName);
  //
  // final Object oldValue = current.field(fieldName);
  // if (oldValue != null) {
  // if (oldValue instanceof Collection<?>) {
  // if (fieldValue instanceof Collection<?>)
  // if (!OMultiValue.equals((Collection<Object>) oldValue, (Collection<Object>) fieldValue))
  // ((Collection<Object>) oldValue).addAll((Collection<? extends Object>) fieldValue);
  // else
  // continue;
  // else {
  // ((Collection<Object>) oldValue).add(fieldValue);
  // fieldValue = oldValue;
  // }
  // }
  // }
  //
  // // OVERWRITE IT
  // current.field(fieldName, fieldValue);
  // }
  //
  // record = current;
  // context.setVariable("currentRecord", record);
  //
  // recordsCreated--;
  //
  // recordsUpdated++;
  // }
  // }
  //
  // iConfiguration.getImplementation().onBeforeLine(db, context);
  //
  // record.unpin();
  //
  // if (iConfiguration.isAutoSave())
  // try {
  // record.save();
  // } catch (Exception e) {
  // OLogManager.instance().error(this, "Error on saving record: " + e);
  // throw e;
  // }
  //
  // recordsImportedInFile++;
  // recordsImported++;
  // warnings = (Integer) context.getVariable("warnings");
  // errors = (Integer) context.getVariable("errors");
  //
  // iConfiguration.getImplementation().onAfterLine(db, context);
  // } catch (Exception e) {
  // OLogManager.instance().error(this, "Error on importing line, continue with the rest of the file", e);
  // errors++;
  // }
  // }
  //
  // private void parseFields(final ODatabaseDocumentTx db, final OTransformerConfiguration iConfiguration, final ODocument record)
  // {
  //
  // if (iConfiguration.getClassName() != null && !record.getClassName().equals(iConfiguration.getClassName()))
  // record.setClassName(iConfiguration.getClassName());
  //
  // while (iFieldNames.hasNext() && iFieldValues.hasNext()) {
  // final String fieldName = iFieldNames.next();
  // Object fieldValue = iFieldValues.next();
  //
  // final OType fieldType = iConfiguration.getFieldType(fieldName);
  //
  // if (fieldType == null)
  // continue;
  //
  // if (fieldValue != null) {
  // if (iConfiguration.getTrims().contains(fieldName))
  // // TRIM IT
  // fieldValue = fieldValue.toString().trim();
  //
  // fieldValue = OStringSerializerHelper.getStringContent(fieldValue);
  // }
  //
  // try {
  // if (fieldValue != null) {
  // if (fieldValue.toString().length() == 0 || fieldValue.toString().equals(data.nullValue))
  // fieldValue = null;
  // else {
  // final String joinTargetField = iConfiguration.getFieldTargetJoin(fieldName);
  // final String edgeClass = iConfiguration.getFieldEdge(fieldName);
  //
  // if (joinTargetField != null) {
  // fieldValue = join(db, record, fieldValue, iConfiguration);
  // recordsJoined++;
  // } else if (fieldType == OType.DATE || fieldType == OType.DATETIME) {
  // final SimpleDateFormat df = new SimpleDateFormat(iConfiguration.getFieldFormatDetail(fieldName));
  // df.setTimeZone(TimeZone.getTimeZone("UTC"));
  // fieldValue = df.parse((String) fieldValue);
  // } else {
  // fieldValue = OType.convert(fieldValue, fieldType.getDefaultJavaType());
  // }
  // }
  // }
  //
  // record.field(fieldName, fieldValue);
  // } catch (Exception e) {
  // OLogManager.instance().info(this, "* Error on row %d field %s", e, currentLine, fieldName);
  // continue;
  // }
  // }
  // }

}
