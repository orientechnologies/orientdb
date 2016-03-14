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
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.common.util.OTriple;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.OTrackedMap;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OSecurity;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.filter.OSQLFilter;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItem;
import com.orientechnologies.orient.core.sql.parser.OUpdateStatement;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.core.storage.OStorage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * SQL UPDATE command.
 * 
 * @author Luca Garulli
 * 
 */
public class OCommandExecutorSQLUpdate extends OCommandExecutorSQLRetryAbstract implements OCommandDistributedReplicateRequest,
    OCommandResultListener {
  public static final String                    KEYWORD_UPDATE    = "UPDATE";
  private static final String                   KEYWORD_ADD       = "ADD";
  private static final String                   KEYWORD_PUT       = "PUT";
  private static final String                   KEYWORD_REMOVE    = "REMOVE";
  private static final String                   KEYWORD_INCREMENT = "INCREMENT";
  private static final String                   KEYWORD_MERGE     = "MERGE";
  private static final String                   KEYWORD_UPSERT    = "UPSERT";
  private static final Object                   EMPTY_VALUE       = new Object();
  private List<OPair<String, Object>>           setEntries        = new ArrayList<OPair<String, Object>>();
  private List<OPair<String, Object>>           addEntries        = new ArrayList<OPair<String, Object>>();
  private List<OTriple<String, String, Object>> putEntries        = new ArrayList<OTriple<String, String, Object>>();
  private List<OPair<String, Object>>           removeEntries     = new ArrayList<OPair<String, Object>>();
  private List<OPair<String, Object>>           incrementEntries  = new ArrayList<OPair<String, Object>>();
  private ODocument                             merge             = null;
  private String                                lockStrategy      = "NONE";
  private OReturnHandler                        returnHandler     = new ORecordCountHandler();
  private OQuery<?>                             query;
  private OSQLFilter                            compiledFilter;
  private String                                subjectName;
  private OCommandParameters                    parameters;
  private boolean                               upsertMode        = false;
  private boolean                               isUpsertAllowed   = false;
  private boolean                               updated           = false;
  private OClass                                clazz             = null;
  private DISTRIBUTED_EXECUTION_MODE            distributedMode;

  @SuppressWarnings("unchecked")
  public OCommandExecutorSQLUpdate parse(final OCommandRequest iRequest) {
    final OCommandRequestText textRequest = (OCommandRequestText) iRequest;

    String queryText = textRequest.getText();
    String originalQuery = queryText;
    try {
      queryText = preParse(queryText, iRequest);
      textRequest.setText(queryText);

      final ODatabaseDocument database = getDatabase();

      init((OCommandRequestText) iRequest);

      setEntries.clear();
      addEntries.clear();
      putEntries.clear();
      removeEntries.clear();
      incrementEntries.clear();
      content = null;
      merge = null;

      query = null;

      parserRequiredKeyword(KEYWORD_UPDATE);

      subjectName = parserRequiredWord(false, "Invalid target", " =><,\r\n");
      if (subjectName == null)
        throwSyntaxErrorException("Invalid subject name. Expected cluster, class, index or sub-query");

      clazz = extractClassFromTarget(subjectName);

      String word = parserNextWord(true);

      if (parserIsEnded()
          || (!word.equals(KEYWORD_SET) && !word.equals(KEYWORD_ADD) && !word.equals(KEYWORD_PUT) && !word.equals(KEYWORD_REMOVE)
              && !word.equals(KEYWORD_INCREMENT) && !word.equals(KEYWORD_CONTENT) && !word.equals(KEYWORD_MERGE)
              && !word.equals(KEYWORD_LOCK) && !word.equals(KEYWORD_RETURN) && !word.equals(KEYWORD_UPSERT)))
        throwSyntaxErrorException("Expected keyword " + KEYWORD_SET + "," + KEYWORD_ADD + "," + KEYWORD_CONTENT + ","
            + KEYWORD_MERGE + "," + KEYWORD_PUT + "," + KEYWORD_REMOVE + "," + KEYWORD_INCREMENT + "," + KEYWORD_LOCK + " or "
            + KEYWORD_RETURN + " or " + KEYWORD_UPSERT);

      while ((!parserIsEnded() && !parserGetLastWord().equals(OCommandExecutorSQLAbstract.KEYWORD_WHERE))
          || parserGetLastWord().equals(KEYWORD_UPSERT)) {
        word = parserGetLastWord();

        if (word.equals(KEYWORD_CONTENT))
          parseContent();
        else if (word.equals(KEYWORD_MERGE))
          parseMerge();
        else if (word.equals(KEYWORD_SET))
          parseSetFields(clazz, setEntries);
        else if (word.equals(KEYWORD_ADD))
          parseAddFields();
        else if (word.equals(KEYWORD_PUT))
          parsePutFields();
        else if (word.equals(KEYWORD_REMOVE))
          parseRemoveFields();
        else if (word.equals(KEYWORD_INCREMENT))
          parseIncrementFields();
        else if (word.equals(KEYWORD_LOCK))
          lockStrategy = parseLock();
        else if (word.equals(KEYWORD_UPSERT))
          upsertMode = true;
        else if (word.equals(KEYWORD_RETURN))
          parseReturn();
        else if (word.equals(KEYWORD_RETRY))
          parseRetry();
        else
          break;

        parserNextWord(true);
      }

      final String additionalStatement = parserGetLastWord();

      if (subjectName.startsWith("(")) {
        subjectName = subjectName.trim();
        query = database.command(new OSQLAsynchQuery<ODocument>(subjectName.substring(1, subjectName.length() - 1), this)
            .setContext(context));

        if (additionalStatement.equals(OCommandExecutorSQLAbstract.KEYWORD_WHERE)
            || additionalStatement.equals(OCommandExecutorSQLAbstract.KEYWORD_LIMIT))
          compiledFilter = OSQLEngine.getInstance().parseCondition(parserText.substring(parserGetCurrentPosition()), getContext(),
              KEYWORD_WHERE);

      } else if (additionalStatement.equals(OCommandExecutorSQLAbstract.KEYWORD_WHERE)
          || additionalStatement.equals(OCommandExecutorSQLAbstract.KEYWORD_LIMIT)
          || additionalStatement.equals(OCommandExecutorSQLAbstract.KEYWORD_LET) || additionalStatement.equals(KEYWORD_LOCK)) {
        if (this.preParsedStatement != null) {
          Map<Object, Object> params = ((OCommandRequestText) iRequest).getParameters();
          OUpdateStatement updateStm = (OUpdateStatement) preParsedStatement;
          StringBuilder selectString = new StringBuilder();
          selectString.append("select from ");
          selectString.append(updateStm.target.toString());
          if (updateStm.whereClause != null) {
            selectString.append(" WHERE ");
            selectString.append(updateStm.whereClause.toString());
          }
          if (updateStm.limit != null) {
            selectString.append(" ");
            selectString.append(updateStm.limit.toString());
          }
          if(updateStm.timeout!=null){
            selectString.append(" ");
            selectString.append(updateStm.timeout.toString());
          }
          if(updateStm.lockRecord) {
            selectString.append(" LOCK RECORD");
          }

          query = new OSQLAsynchQuery<ODocument>(selectString.toString(), this);
        } else {
          query = new OSQLAsynchQuery<ODocument>("select from " + getSelectTarget() + " " + additionalStatement + " " + parserText
              .substring(parserGetCurrentPosition()), this);
        }

        isUpsertAllowed = (((OMetadataInternal) getDatabase().getMetadata()).getImmutableSchemaSnapshot().getClass(subjectName) != null);
      } else if (!additionalStatement.isEmpty())
        throwSyntaxErrorException("Invalid keyword " + additionalStatement);
      else
        query = new OSQLAsynchQuery<ODocument>("select from " + getSelectTarget() , this);


      if (upsertMode && !isUpsertAllowed)
        throwSyntaxErrorException("Upsert only works with class names ");

      if (upsertMode && !additionalStatement.equals(OCommandExecutorSQLAbstract.KEYWORD_WHERE))
        throwSyntaxErrorException("Upsert only works with WHERE keyword");
    } finally {
      textRequest.setText(originalQuery);
    }

    return this;
  }

  private String getSelectTarget() {
    if(preParsedStatement == null){
      return subjectName;
    }
    return ((OUpdateStatement)preParsedStatement).target.toString();
  }

  public Object execute(final Map<Object, Object> iArgs) {
    if (subjectName == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    parameters = new OCommandParameters(iArgs);
    Map<Object, Object> queryArgs;
    if (parameters.size() > 0 && parameters.getByName(0) != null) {
      queryArgs = new HashMap<Object, Object>();
      for (int i = parameterCounter; i < parameters.size(); i++) {
        if (parameters.getByName(i) != null)
          queryArgs.put(i - parameterCounter, parameters.getByName(i));
      }
    } else {
      queryArgs = iArgs;
    }

    query.setUseCache(false);
    query.setContext(context);

    returnHandler.reset();

    if (lockStrategy.equals("RECORD"))
      query.getContext().setVariable("$locking", OStorage.LOCKING_STRATEGY.EXCLUSIVE_LOCK);

    for (int r = 0; r < retry; ++r) {
      try {

        getDatabase().query(query, queryArgs);

        break;

      } catch (OConcurrentModificationException e) {
        if (r + 1 >= retry)
          // NO RETRY; PROPAGATE THE EXCEPTION
          throw e;

        // RETRY?
        if (wait > 0)
          try {
            Thread.sleep(wait);
          } catch (InterruptedException ignored) {
          }
      }
    }

    if (upsertMode && !updated) {
      // IF UPDATE DOES NOT PRODUCE RESULTS AND UPSERT MODE IS ENABLED, CREATE DOCUMENT AND APPLY SET/ADD/PUT/MERGE and so on
      final ODocument doc = subjectName != null ? new ODocument(subjectName) : new ODocument();
      final String suspendedLockStrategy = lockStrategy;
      lockStrategy = "NONE";// New record hasn't been created under exclusive lock - just to avoid releasing locks by result(doc)
      try {
        result(doc);
      } catch (ORecordDuplicatedException e) {
        if (upsertMode)
          // UPDATE THE NEW RECORD
          getDatabase().query(query, queryArgs);
        else
          throw e;
      } catch (ORecordNotFoundException e) {
        if (upsertMode)
          // UPDATE THE NEW RECORD
          getDatabase().query(query, queryArgs);
        else
          throw e;
      } catch (OConcurrentModificationException e) {
        if (upsertMode)
          // UPDATE THE NEW RECORD
          getDatabase().query(query, queryArgs);
        else
          throw e;
      }

      lockStrategy = suspendedLockStrategy;
    }

    return returnHandler.ret();
  }

  /**
   * Update current record.
   */
  @SuppressWarnings("unchecked")
  public boolean result(final Object iRecord) {
    final ODocument record = ((OIdentifiable) iRecord).getRecord();

    if (compiledFilter != null) {
      // ADDITIONAL FILTERING
      if (!(Boolean) compiledFilter.evaluate(record, null, context))
        return false;
    }

    parameters.reset();

    returnHandler.beforeUpdate(record);

    boolean updated = handleContent(record);
    updated |= handleMerge(record);
    updated |= handleSetEntries(record);
    updated |= handleIncrementEntries(record);
    updated |= handleAddEntries(record);
    updated |= handlePutEntries(record);
    updated |= handleRemoveEntries(record);

    if (updated) {
      record.setDirty();
      record.save();
      returnHandler.afterUpdate(record);
      this.updated = true;
    }

    return true;
  }

  @Override
  public String getSyntax() {
    return "UPDATE <class>|cluster:<cluster>> [SET|ADD|PUT|REMOVE|INCREMENT|CONTENT {<JSON>}|MERGE {<JSON>}] [[,] <field-name> = <expression>|<sub-command>]* [LOCK <NONE|RECORD>] [UPSERT] [RETURN <COUNT|BEFORE|AFTER>] [WHERE <conditions>]";
  }

  @Override
  public OCommandDistributedReplicateRequest.DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    if (distributedMode == null)
      // REPLICATE MODE COULD BE MORE EFFICIENT ON MASSIVE UPDATES
      distributedMode = upsertMode || query == null || getDatabase().getTransaction().isActive() ? DISTRIBUTED_EXECUTION_MODE.LOCAL
          : DISTRIBUTED_EXECUTION_MODE.REPLICATE;
    return distributedMode;
  }

  @Override
  public DISTRIBUTED_RESULT_MGMT getDistributedResultManagement() {
    return distributedMode == DISTRIBUTED_EXECUTION_MODE.LOCAL ? DISTRIBUTED_RESULT_MGMT.CHECK_FOR_EQUALS
        : DISTRIBUTED_RESULT_MGMT.MERGE;
  }

  @Override
  public void end() {
  }

  @Override
  public int getSecurityOperationType() {
    return ORole.PERMISSION_UPDATE;
  }

  protected void parseMerge() {
    if (!parserIsEnded() && !parserGetLastWord().equals(KEYWORD_WHERE)) {
      final String contentAsString = parserRequiredWord(false, "document to merge expected").trim();
      merge = new ODocument().fromJSON(contentAsString);
      parserSkipWhiteSpaces();
    }

    if (merge == null)
      throwSyntaxErrorException("Document to merge not provided. Example: MERGE { \"name\": \"Jay\" }");
  }

  protected String getBlock(String fieldValue) {
    final int startPos = parserGetCurrentPosition();

    if (fieldValue.startsWith("{") || fieldValue.startsWith("[")) {
      if (startPos > 0)
        parserSetCurrentPosition(startPos - fieldValue.length());
      else
        parserSetCurrentPosition(parserText.length() - fieldValue.length());

      parserSkipWhiteSpaces();
      final StringBuilder buffer = new StringBuilder();
      parserSetCurrentPosition(OStringSerializerHelper.parse(parserText, buffer, parserGetCurrentPosition(), -1,
          OStringSerializerHelper.DEFAULT_FIELD_SEPARATOR, true, true, false, -1, false,
          OStringSerializerHelper.DEFAULT_IGNORE_CHARS));
      fieldValue = buffer.toString();
    }
    return fieldValue;
  }

  /**
   * Parses the returning keyword if found.
   */
  protected void parseReturn() throws OCommandSQLParsingException {
    parserNextWord(false, " ");
    String mode = parserGetLastWord().trim();

    if (mode.equalsIgnoreCase("COUNT")) {
      returnHandler = new ORecordCountHandler();
    } else if (mode.equalsIgnoreCase("BEFORE") || mode.equalsIgnoreCase("AFTER")) {

      parserNextWord(false, " ");
      String returning = parserGetLastWord().trim();
      Object returnExpression = null;
      if (returning.equalsIgnoreCase(KEYWORD_WHERE) || returning.equalsIgnoreCase(KEYWORD_TIMEOUT)
          || returning.equalsIgnoreCase(KEYWORD_LIMIT) || returning.equalsIgnoreCase(KEYWORD_UPSERT)
          || returning.equalsIgnoreCase(KEYWORD_LOCK) || returning.length() == 0) {
        parserGoBack();
      } else {
        if (returning.startsWith("$") || returning.startsWith("@"))
          returnExpression = (returning.length() > 0) ? OSQLHelper.parseValue(this, returning, this.getContext()) : null;
        else
          throwSyntaxErrorException("record attribute (@attributes) or functions with $current variable expected");
      }

      if (mode.equalsIgnoreCase("BEFORE"))
        returnHandler = new OOriginalRecordsReturnHandler(returnExpression, getContext());
      else
        returnHandler = new OUpdatedRecordsReturnHandler(returnExpression, getContext());

    } else
      throwSyntaxErrorException(" COUNT | BEFORE | AFTER keywords expected");
  }

  private boolean handleContent(ODocument record) {
    boolean updated = false;
    if (content != null) {
      // REPLACE ALL THE CONTENT

      final OClass restricted = getDatabase().getMetadata().getSchema().getClass(OSecurity.RESTRICTED_CLASSNAME);

      final ODocument restrictedFields = new ODocument();
      if (restricted != null) {
        for (OProperty prop : restricted.properties()) {
          restrictedFields.field(prop.getName(), record.field(prop.getName()));
        }

        OClass recordClass = ODocumentInternal.getImmutableSchemaClass(record);
        if (recordClass != null && recordClass.isSubClassOf("V")) {
          for (String fieldName : record.fieldNames()) {
            if (fieldName.startsWith("in_") || fieldName.startsWith("out_")) {
              restrictedFields.field(fieldName, record.field(fieldName));
            }
          }
        } else if (recordClass != null && recordClass.isSubClassOf("E")) {
          for (String fieldName : record.fieldNames()) {
            if (fieldName.equals("in") || fieldName.equals("out")) {
              restrictedFields.field(fieldName, record.field(fieldName));
            }
          }
        }
      }

      record.merge(restrictedFields, false, false);
      record.merge(content, true, false);
      updated = true;
    }
    return updated;
  }

  private boolean handleMerge(ODocument record) {
    boolean updated = false;
    if (merge != null) {
      // MERGE THE CONTENT
      record.merge(merge, true, false);
      updated = true;
    }
    return updated;
  }

  private boolean handleSetEntries(final ODocument record) {
    boolean updated = false;
    // BIND VALUES TO UPDATE
    if (!setEntries.isEmpty()) {
      OSQLHelper.bindParameters(record, setEntries, parameters, context);
      updated = true;
    }
    return updated;
  }

  private boolean handleIncrementEntries(final ODocument record) {
    boolean updated = false;
    // BIND VALUES TO INCREMENT
    if (!incrementEntries.isEmpty()) {
      for (OPair<String, Object> entry : incrementEntries) {
        final Number prevValue = record.field(entry.getKey());

        Number current;
        if (entry.getValue() instanceof OSQLFilterItem)
          current = (Number) ((OSQLFilterItem) entry.getValue()).getValue(record, null, context);
        else if (entry.getValue() instanceof Number)
          current = (Number) entry.getValue();
        else
          throw new OCommandExecutionException("Increment value is not a number (" + entry.getValue() + ")");

        if (prevValue == null)
          // NO PREVIOUS VALUE: CONSIDER AS 0
          record.field(entry.getKey(), current);
        else
          // COMPUTING INCREMENT
          record.field(entry.getKey(), OType.increment(prevValue, current));
      }
      updated = true;
    }
    return updated;
  }

  private boolean handleAddEntries(ODocument record) {
    boolean updated = false;
    // BIND VALUES TO ADD
    Object fieldValue;
    for (OPair<String, Object> entry : addEntries) {
      Collection<Object> coll = null;
      ORidBag bag = null;
      if (!record.containsField(entry.getKey())) {
        // GET THE TYPE IF ANY
        if (ODocumentInternal.getImmutableSchemaClass(record) != null) {
          OProperty prop = ODocumentInternal.getImmutableSchemaClass(record).getProperty(entry.getKey());
          if (prop != null && prop.getType() == OType.LINKSET)
            // SET TYPE
            coll = new HashSet<Object>();
          if (prop != null && prop.getType() == OType.LINKBAG) {
            // there is no ridbag value already but property type is defined as LINKBAG
            bag = new ORidBag();
            bag.setOwner(record);
            record.field(entry.getKey(), bag);
          }
        }
        if (coll == null && bag == null)
          // IN ALL OTHER CASES USE A LIST
          coll = new ArrayList<Object>();
        if (coll != null) {
          // containField's condition above does NOT check subdocument's fields so
          Collection<Object> currColl = record.field(entry.getKey());
          if (currColl == null) {
            record.field(entry.getKey(), coll);
            coll = record.field(entry.getKey());
          } else
            coll = currColl;
        }

      } else {
        fieldValue = record.field(entry.getKey());

        if (fieldValue instanceof Collection<?>)
          coll = (Collection<Object>) fieldValue;
        else if (fieldValue instanceof ORidBag)
          bag = (ORidBag) fieldValue;
        else
          continue;
      }

      final Object value = extractValue(record, entry);

      if (coll != null) {
        if (value instanceof OIdentifiable)
          coll.add(value);
        else
          OMultiValue.add(coll, value);
      } else {
        if (!(value instanceof OIdentifiable))
          throw new OCommandExecutionException("Only links or records can be added to LINKBAG");

        bag.add((OIdentifiable) value);
      }
      updated = true;
    }
    return updated;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private boolean handlePutEntries(ODocument record) {
    boolean updated = false;
    if (!putEntries.isEmpty()) {
      // BIND VALUES TO PUT (AS MAP)
      for (OTriple<String, String, Object> entry : putEntries) {
        Object fieldValue = record.field(entry.getKey());

        if (fieldValue == null) {
          if (ODocumentInternal.getImmutableSchemaClass(record) != null) {
            final OProperty property = ODocumentInternal.getImmutableSchemaClass(record).getProperty(entry.getKey());
            if (property != null
                && (property.getType() != null && (!property.getType().equals(OType.EMBEDDEDMAP) && !property.getType().equals(
                    OType.LINKMAP)))) {
              throw new OCommandExecutionException("field " + entry.getKey() + " is not defined as a map");
            }
          }
          fieldValue = new HashMap<String, Object>();
          record.field(entry.getKey(), fieldValue);
        }

        if (fieldValue instanceof Map<?, ?>) {
          Map<String, Object> map = (Map<String, Object>) fieldValue;

          OPair<String, Object> pair = entry.getValue();

          Object value = extractValue(record, pair);

          if (record.getSchemaClass() != null) {
            final OProperty property = record.getSchemaClass().getProperty(entry.getKey());
            if (property != null && property.getType().equals(OType.LINKMAP) && !(value instanceof OIdentifiable)) {
              throw new OCommandExecutionException("field " + entry.getKey() + " defined of type LINKMAP accept only link values");
            }
          }
          if (OType.LINKMAP.equals(OType.getTypeByValue(fieldValue)) && !(value instanceof OIdentifiable)) {
            map = new OTrackedMap(record, map, Object.class);
            record.field(entry.getKey(), map, OType.EMBEDDEDMAP);
          }
          map.put(pair.getKey(), value);
          updated = true;
        }
      }
    }
    return updated;
  }

  private boolean handleRemoveEntries(ODocument record) {
    boolean updated = false;
    if (!removeEntries.isEmpty()) {
      // REMOVE FIELD IF ANY
      for (OPair<String, Object> entry : removeEntries) {
        Object value = extractValue(record, entry);

        if (value == EMPTY_VALUE) {
          record.removeField(entry.getKey());
          updated = true;
        } else {
          final Object fieldValue = record.field(entry.getKey());

          if (fieldValue instanceof Collection<?>) {
            updated = removeFromCollection(updated, value, (Collection<?>) fieldValue);
          } else if (fieldValue instanceof Map<?, ?>) {
            updated = removeFromMap(updated, value, (Map<?, ?>) fieldValue);
          } else if (fieldValue instanceof ORidBag) {
            updated = removeFromBag(record, updated, value, (ORidBag) fieldValue);
          }
        }
      }
    }
    return updated;
  }

  private boolean removeFromCollection(boolean updated, Object value, Collection<?> collection) {
    if (value instanceof Collection<?>)
      updated |= collection.removeAll(((Collection) value));
    else
      updated |= collection.remove(value);
    return updated;
  }

  private boolean removeFromMap(boolean updated, Object value, Map<?, ?> map) {
    if (value instanceof Collection) {
      for (Object o : ((Collection) value)) {
        updated |= map.remove(o) != null;
      }
    } else
      updated |= map.remove(value) != null;
    return updated;
  }

  private boolean removeFromBag(ODocument record, boolean updated, Object value, ORidBag bag) {
    if (value instanceof Collection) {
      for (Object o : ((Collection) value)) {
        updated |= removeSingleValueFromBag(bag, o, record);
      }
    } else
      updated |= removeSingleValueFromBag(bag, value, record);
    return updated;
  }

  private boolean removeSingleValueFromBag(ORidBag bag, Object value, ODocument record) {
    if (!(value instanceof OIdentifiable))
      throw new OCommandExecutionException("Only links or records can be removed from LINKBAG");

    bag.remove((OIdentifiable) value);
    return record.isDirty();
  }

  private Object extractValue(ODocument record, OPair<String, Object> entry) {
    Object value = entry.getValue();

    if (value instanceof OSQLFilterItem)
      value = ((OSQLFilterItem) value).getValue(record, null, context);
    else if (value instanceof OCommandRequest)
      value = ((OCommandRequest) value).execute(record, null, context);

    if (value instanceof OIdentifiable && ((OIdentifiable) value).getIdentity().isPersistent())
      // USE ONLY THE RID TO AVOID CONCURRENCY PROBLEM WITH OLD VERSIONS
      value = ((OIdentifiable) value).getIdentity();
    return value;
  }

  private void parseAddFields() {
    String fieldName;
    String fieldValue;

    boolean firstLap = true;
    while (!parserIsEnded() && (firstLap || parserGetLastSeparator() == ',' || parserGetCurrentChar() == ',')
        && !parserGetLastWord().equals(KEYWORD_WHERE)) {

      fieldName = parserRequiredWord(false, "Field name expected");
      parserRequiredKeyword("=");
      fieldValue = parserRequiredWord(false, "Value expected", " =><,\r\n");

      final Object v = convertValue(clazz, fieldName, getFieldValueCountingParameters(fieldValue));

      // INSERT TRANSFORMED FIELD VALUE
      addEntries.add(new OPair<String, Object>(fieldName, v));
      parserSkipWhiteSpaces();

      firstLap = false;
    }

    if (addEntries.size() == 0)
      throwSyntaxErrorException("Entries to add <field> = <value> are missed. Example: name = 'Bill', salary = 300.2.");
  }

  private void parsePutFields() {
    String fieldName;
    String fieldKey;
    String fieldValue;

    boolean firstLap = true;
    while (!parserIsEnded() && (firstLap || parserGetLastSeparator() == ',' || parserGetCurrentChar() == ',')
        && !parserGetLastWord().equals(KEYWORD_WHERE)) {

      fieldName = parserRequiredWord(false, "Field name expected");
      parserRequiredKeyword("=");
      fieldKey = parserRequiredWord(false, "Key expected");
      fieldValue = getBlock(parserRequiredWord(false, "Value expected", " =><,\r\n"));

      // INSERT TRANSFORMED FIELD VALUE
      putEntries.add(new OTriple(fieldName, (String) getFieldValueCountingParameters(fieldKey),
          getFieldValueCountingParameters(fieldValue)));
      parserSkipWhiteSpaces();

      firstLap = false;
    }

    if (putEntries.size() == 0)
      throwSyntaxErrorException("Entries to put <field> = <key>, <value> are missed. Example: name = 'Bill', 30");
  }

  private void parseRemoveFields() {
    String fieldName;
    String fieldValue;
    Object value;

    boolean firstLap = true;
    while (!parserIsEnded() && (firstLap || parserGetLastSeparator() == ',' || parserGetCurrentChar() == ',')
        && !parserGetLastWord().equals(KEYWORD_WHERE)) {

      fieldName = parserRequiredWord(false, "Field name expected");
      final boolean found = parserOptionalKeyword("=", "WHERE", "RETURN", "LOCK", "LIMIT");
      if (found)
        if (parserGetLastWord().equals("=")) {
          fieldValue = getBlock(parserRequiredWord(false, "Value expected", " =><,\r\n"));
          value = getFieldValueCountingParameters(fieldValue);
        } else {
          parserGoBack();
          value = EMPTY_VALUE;
        }
      else
        value = EMPTY_VALUE;

      // INSERT FIELD NAME TO BE REMOVED
      removeEntries.add(new OPair<String, Object>(fieldName, value));
      parserSkipWhiteSpaces();

      firstLap = false;
    }

    if (removeEntries.size() == 0)
      throwSyntaxErrorException("Field(s) to remove are missed. Example: name, salary");
  }

  private void parseIncrementFields() {
    String fieldName;
    String fieldValue;

    boolean firstLap = true;
    while (!parserIsEnded() && (firstLap || parserGetLastSeparator() == ',') && !parserGetLastWord().equals(KEYWORD_WHERE)) {

      fieldName = parserRequiredWord(false, "Field name expected");
      parserRequiredKeyword("=");
      fieldValue = getBlock(parserRequiredWord(false, "Value expected"));

      // INSERT TRANSFORMED FIELD VALUE
      incrementEntries.add(new OPair(fieldName, getFieldValueCountingParameters(fieldValue)));
      parserSkipWhiteSpaces();

      firstLap = false;
    }

    if (incrementEntries.size() == 0)
      throwSyntaxErrorException("Entries to increment <field> = <value> are missed. Example: salary = -100");
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.WRITE;
  }
}
