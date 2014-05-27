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
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.filter.OSQLFilter;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItem;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionRuntime;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.core.storage.OStorage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;


/**
 * SQL UPDATE command.
 * 
 * @author Luca Garulli
 * 
 */
public class OCommandExecutorSQLUpdate extends OCommandExecutorSQLRetryAbstract implements OCommandDistributedReplicateRequest,
    OCommandResultListener {
  public static final String                 KEYWORD_UPDATE    = "UPDATE";
  private static final String                KEYWORD_ADD       = "ADD";
  private static final String                KEYWORD_PUT       = "PUT";
  private static final String                KEYWORD_REMOVE    = "REMOVE";
  private static final String                KEYWORD_INCREMENT = "INCREMENT";
  private static final String                KEYWORD_MERGE     = "MERGE";
  private static final String                KEYWORD_UPSERT    = "UPSERT";
  private static final Object                EMPTY_VALUE       = new Object();
  private Map<String, Object>                setEntries        = new LinkedHashMap<String, Object>();
  private List<OPair<String, Object>>        addEntries        = new ArrayList<OPair<String, Object>>();
  private Map<String, OPair<String, Object>> putEntries        = new LinkedHashMap<String, OPair<String, Object>>();
  private List<OPair<String, Object>>        removeEntries     = new ArrayList<OPair<String, Object>>();
  private Map<String, Number>                incrementEntries  = new LinkedHashMap<String, Number>();
  private ODocument                          merge             = null;
  private String                             lockStrategy      = "NONE";
  private Object                             returnExpression = null;
  private SQLUpdateReturnModeEnum            returnMode          = SQLUpdateReturnModeEnum.COUNT;
  private List<ORecord<?>>                   allUpdatedRecords;
  private OQuery<?>                          query;
  private OSQLFilter                         compiledFilter;
  private int                                recordCount       = 0;
  private String                             subjectName;
  private OCommandParameters                 parameters;
  private boolean                            upsertMode        = false;
  private boolean                            isUpsertAllowed   = false;

  @SuppressWarnings("unchecked")
  public OCommandExecutorSQLUpdate parse(final OCommandRequest iRequest) {
    final ODatabaseRecord database = getDatabase();

    init((OCommandRequestText) iRequest);

    setEntries.clear();
    addEntries.clear();
    putEntries.clear();
    removeEntries.clear();
    incrementEntries.clear();
    content = null;
    merge = null;

    query = null;
    recordCount = 0;

    parserRequiredKeyword(KEYWORD_UPDATE);

    subjectName = parserRequiredWord(false, "Invalid target", " =><,\r\n");
    if (subjectName == null)
      throwSyntaxErrorException("Invalid subject name. Expected cluster, class, index or sub-query");

    parserNextWord(true);
    String word = parserGetLastWord();

    if (parserIsEnded()
        || (!word.equals(KEYWORD_SET) && !word.equals(KEYWORD_ADD) && !word.equals(KEYWORD_PUT) && !word.equals(KEYWORD_REMOVE)
            && !word.equals(KEYWORD_INCREMENT) && !word.equals(KEYWORD_CONTENT) && !word.equals(KEYWORD_MERGE)
            && !word.equals(KEYWORD_LOCK) && !word.equals(KEYWORD_RETURN) && !word.equals(KEYWORD_UPSERT)))
      throwSyntaxErrorException("Expected keyword " + KEYWORD_SET + "," + KEYWORD_ADD + "," + KEYWORD_CONTENT + "," + KEYWORD_MERGE
          + "," + KEYWORD_PUT + "," + KEYWORD_REMOVE + "," + KEYWORD_INCREMENT + "," + KEYWORD_LOCK + " or " + KEYWORD_RETURN
          + " or " + KEYWORD_UPSERT);

    while ((!parserIsEnded() && !parserGetLastWord().equals(OCommandExecutorSQLAbstract.KEYWORD_WHERE))
        || parserGetLastWord().equals(KEYWORD_UPSERT)) {
      word = parserGetLastWord();

      if (word.equals(KEYWORD_CONTENT))
        parseContent();
      else if (word.equals(KEYWORD_MERGE))
        parseMerge();
      else if (word.equals(KEYWORD_SET))
        parseSetFields(setEntries);
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
        returnMode = parseReturn();
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
      query = new OSQLAsynchQuery<ODocument>("select from " + subjectName + " " + additionalStatement + " "
          + parserText.substring(parserGetCurrentPosition()), this);
      isUpsertAllowed = (getDatabase().getMetadata().getSchema().getClass(subjectName) != null);
    } else if (additionalStatement != null && !additionalStatement.isEmpty())
      throwSyntaxErrorException("Invalid keyword " + additionalStatement);
    else
      query = new OSQLAsynchQuery<ODocument>("select from " + subjectName, this);

    if (upsertMode && !isUpsertAllowed)
      throwSyntaxErrorException("Upsert only works with class names ");

    if (upsertMode && !additionalStatement.equals(OCommandExecutorSQLAbstract.KEYWORD_WHERE))
      throwSyntaxErrorException("Upsert only works with WHERE keyword");

    return this;
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

    allUpdatedRecords = new ArrayList<ORecord<?>>();

    if (lockStrategy.equals("RECORD"))
      query.getContext().setVariable("$locking", OStorage.LOCKING_STRATEGY.KEEP_EXCLUSIVE_LOCK);

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
          } catch (InterruptedException e1) {
          }
      }
    }

    // IF UPDATE DOES NOT PRODUCE RESULTS AND UPSERT MODE IS ENABLED, CREATE DOCUMENT AND APPLY SET/ADD/PUT/MERGE and so on
    if (upsertMode && recordCount == 0) {
      final ODocument doc = subjectName != null ? new ODocument(subjectName) : new ODocument();
      final String suspendedLockStrategy = lockStrategy;
      lockStrategy = "NONE";// New record hasn't been created under exlusive lock - just to avoid releasing locks by result(doc)
      result(doc);
      lockStrategy = suspendedLockStrategy;
    }

    if (returnMode== SQLUpdateReturnModeEnum.COUNT)
      // RETURNS ONLY THE COUNT
      return recordCount;
    else
      if (returnExpression ==null)
      {
          return allUpdatedRecords;
      }
      else
      {
          List<Object> result = new ArrayList<Object>();
          Object itemResult = null;
          ODocument wrappingDoc = null;
          for (ORecord o : allUpdatedRecords)
          {
            this.getContext().setVariable("current",o);
            itemResult = OSQLHelper.getValue(returnExpression, (ODocument) ((OIdentifiable) o).getRecord(), this.getContext());
            // WRAP WITH ODOCUMENT IF NEEDED
            if (itemResult instanceof OIdentifiable)
                    result.add(itemResult);
            else
            {
               wrappingDoc =  new ODocument("result",itemResult);
               wrappingDoc.field("rid",o.getIdentity());// passing record id.In many cases usable on client side
               wrappingDoc.field("version",o.getVersion());// passing record version
               result.add(wrappingDoc);
            }
          }
          return result;
      }
  }

  /**
   * Update current record.
   */
  @SuppressWarnings("unchecked")
  public boolean result(final Object iRecord) {
    final ODocument record = (ODocument) ((OIdentifiable) iRecord).getRecord();

    if (compiledFilter != null) {
      // ADDITIONAL FILTERING
      if (!(Boolean) compiledFilter.evaluate(record, null, context))
        return false;
    }

    final Set<ODocument> updatedRecords = new HashSet<ODocument>();

    parameters.reset();


    if (returnMode == SQLUpdateReturnModeEnum.BEFORE)
      allUpdatedRecords.add(record.copy());
    else
      allUpdatedRecords.add(record);


    if (content != null) {
      // REPLACE ALL THE CONTENT
      record.clear();
      record.merge(content, false, false);
      updatedRecords.add(record);
    }

    if (merge != null) {
      // MERGE THE CONTENT
      record.merge(merge, true, false);
      updatedRecords.add(record);
    }

    // BIND VALUES TO UPDATE
    if (!setEntries.isEmpty()) {
      Set<ODocument> changedDocuments = OSQLHelper.bindParameters(record, setEntries, parameters, context);
      if (changedDocuments != null)
        updatedRecords.addAll(changedDocuments);
    }

    // BIND VALUES TO INCREMENT
    if (!incrementEntries.isEmpty()) {
      for (Map.Entry<String, Number> entry : incrementEntries.entrySet()) {
        final Number prevValue = record.field(entry.getKey());

        if (prevValue == null)
          // NO PREVIOUS VALUE: CONSIDER AS 0
          record.field(entry.getKey(), entry.getValue());
        else
          // COMPUTING INCREMENT
          record.field(entry.getKey(), OType.increment(prevValue, entry.getValue()));
      }
      updatedRecords.add(record);
    }

    Object v;

    // BIND VALUES TO ADD
    Collection<Object> coll = null;
    ORidBag bag = null;
    Object fieldValue;
    for (OPair<String, Object> entry : addEntries) {
      coll = null;
      if (!record.containsField(entry.getKey())) {
        // GET THE TYPE IF ANY
        if (record.getSchemaClass() != null) {
          OProperty prop = record.getSchemaClass().getProperty(entry.getKey());
            if (prop != null && prop.getType() == OType.LINKSET)
            // SET TYPE
            coll = new HashSet<Object>();
            if (prop != null && prop.getType() == OType.LINKBAG)
            {
                // there is no ridbag value already but property type is defined as LINKBAG
                bag = new ORidBag();
                bag.setOwner(record);
                record.field(entry.getKey(),bag);
            }
        }
         if (coll == null && bag==null)
          // IN ALL OTHER CASES USE A LIST
          coll = new ArrayList<Object>();
         if (coll!=null)
         {
            // containField's condition above does NOT check subdocument's fields so
            Collection<Object> currColl = record.field(entry.getKey());
            if (currColl == null)
              record.field(entry.getKey(), coll);
            else
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

      v = entry.getValue();

      if (v instanceof OSQLFilterItem)
        v = ((OSQLFilterItem) v).getValue(record, null, context);
      else if (v instanceof OSQLFunctionRuntime)
        v = ((OSQLFunctionRuntime) v).execute(record, record, null, context);
      else if (v instanceof OCommandRequest)
        v = ((OCommandRequest) v).execute(record, null, context);

      if (v instanceof OIdentifiable)
        // USE ONLY THE RID TO AVOID CONCURRENCY PROBLEM WITH OLD VERSIONS
        v = ((OIdentifiable) v).getIdentity();

      if (coll != null) {
        if (v instanceof OIdentifiable)
          coll.add(v);
        else
          OMultiValue.add(coll, v);
      } else {
        if (!(v instanceof OIdentifiable))
          throw new OCommandExecutionException("Only links or records can be added to LINKBAG");

        bag.add((OIdentifiable) v);
      }
      updatedRecords.add(record);
    }

    Map<String, Object> map;
    OPair<String, Object> pair;

    if (!putEntries.isEmpty()) {
      // BIND VALUES TO PUT (AS MAP)
      for (Entry<String, OPair<String, Object>> entry : putEntries.entrySet()) {
        fieldValue = record.field(entry.getKey());

        if (fieldValue == null) {
          if (record.getSchemaClass() != null) {
            final OProperty property = record.getSchemaClass().getProperty(entry.getKey());
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
          map = (Map<String, Object>) fieldValue;

          pair = entry.getValue();

          v = pair.getValue();

          if (v instanceof OSQLFilterItem)
            v = ((OSQLFilterItem) v).getValue(record, null, context);
          else if (pair.getValue() instanceof OSQLFunctionRuntime)
            v = ((OSQLFunctionRuntime) v).execute(record, record, null, context);
          else if (v instanceof OCommandRequest)
            v = ((OCommandRequest) v).execute(record, null, context);

          if (v instanceof OIdentifiable)
            // USE ONLY THE RID TO AVOID CONCURRENCY PROBLEM WITH OLD VERSIONS
            v = ((OIdentifiable) v).getIdentity();

          map.put(pair.getKey(), v);
          updatedRecords.add(record);
        }
      }
    }

    if (!removeEntries.isEmpty()) {
      // REMOVE FIELD IF ANY
      for (OPair<String, Object> entry : removeEntries) {
        v = entry.getValue();

        if (v instanceof OSQLFilterItem)
          v = ((OSQLFilterItem) v).getValue(record, null, context);
        else if (entry.getValue() instanceof OSQLFunctionRuntime)
          v = ((OSQLFunctionRuntime) v).execute(record, record, null, context);
        else if (v instanceof OCommandRequest)
          v = ((OCommandRequest) v).execute(record, null, context);

        if (v instanceof OIdentifiable)
          // USE ONLY THE RID TO AVOID CONCURRENCY PROBLEM WITH OLD VERSIONS
          v = ((OIdentifiable) v).getIdentity();

        if (v == EMPTY_VALUE) {
          record.removeField(entry.getKey());
          updatedRecords.add(record);
        } else {
          fieldValue = record.field(entry.getKey());

          if (fieldValue instanceof Collection<?>) {
            coll = (Collection<Object>) fieldValue;
            if (coll.remove(v))
              updatedRecords.add(record);
          } else if (fieldValue instanceof Map<?, ?>) {
            map = (Map<String, Object>) fieldValue;
            if (map.remove(v) != null)
              updatedRecords.add(record);
          } else if (fieldValue instanceof ORidBag) {
            bag = (ORidBag) fieldValue;

            if (!(v instanceof OIdentifiable))
              throw new OCommandExecutionException("Only links or records can be removed from LINKBAG");

            bag.remove((OIdentifiable) v);
            if (record.isDirty())
              updatedRecords.add(record);
          }
        }
      }
    }

    for (ODocument d : updatedRecords) {
      d.setDirty();
      d.save();
      recordCount++;
    }

    return true;
  }

  @Override
  public String getSyntax() {
    return "UPDATE <class>|cluster:<cluster>> [SET|ADD|PUT|REMOVE|INCREMENT|CONTENT {<JSON>}|MERGE {<JSON>}] [[,] <field-name> = <expression>|<sub-command>]* [LOCK <NONE|RECORD>] [UPSERT] [RETURN <COUNT|BEFORE|AFTER>] [WHERE <conditions>]";
  }

  @Override
  public void end() {
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

    if (fieldValue.startsWith("{") || fieldValue.startsWith("[") || fieldValue.startsWith("[")) {
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

  private void parseAddFields() {
    String fieldName;
    String fieldValue;

    while (!parserIsEnded() && (addEntries.size() == 0 || parserGetLastSeparator() == ',' || parserGetCurrentChar() == ',')
        && !parserGetLastWord().equals(KEYWORD_WHERE)) {

      fieldName = parserRequiredWord(false, "Field name expected");
      parserRequiredKeyword("=");
      fieldValue = parserRequiredWord(false, "Value expected", " =><,\r\n");

      // INSERT TRANSFORMED FIELD VALUE
      addEntries.add(new OPair<String, Object>(fieldName, getFieldValueCountingParameters(fieldValue)));
      parserSkipWhiteSpaces();
    }

    if (addEntries.size() == 0)
      throwSyntaxErrorException("Entries to add <field> = <value> are missed. Example: name = 'Bill', salary = 300.2.");
  }

  private void parsePutFields() {
    String fieldName;
    String fieldKey;
    String fieldValue;

    while (!parserIsEnded() && (putEntries.size() == 0 || parserGetLastSeparator() == ',' || parserGetCurrentChar() == ',')
        && !parserGetLastWord().equals(KEYWORD_WHERE)) {

      fieldName = parserRequiredWord(false, "Field name expected");
      parserRequiredKeyword("=");
      fieldKey = parserRequiredWord(false, "Key expected");
      fieldValue = getBlock(parserRequiredWord(false, "Value expected", " =><,\r\n"));

      // INSERT TRANSFORMED FIELD VALUE
      putEntries.put(fieldName, new OPair<String, Object>((String) getFieldValueCountingParameters(fieldKey),
          getFieldValueCountingParameters(fieldValue)));
      parserSkipWhiteSpaces();
    }

    if (putEntries.size() == 0)
      throwSyntaxErrorException("Entries to put <field> = <key>, <value> are missed. Example: name = 'Bill', 30");
  }

  private void parseRemoveFields() {
    String fieldName;
    String fieldValue;
    Object value;

    while (!parserIsEnded() && (removeEntries.size() == 0 || parserGetLastSeparator() == ',' || parserGetCurrentChar() == ',')
        && !parserGetLastWord().equals(KEYWORD_WHERE)) {

      fieldName = parserRequiredWord(false, "Field name expected");
      final boolean found = parserOptionalKeyword("=", "WHERE");
      if (found)
        if (parserGetLastWord().equals("WHERE")) {
          parserGoBack();
          value = EMPTY_VALUE;
        } else {
          fieldValue = getBlock(parserRequiredWord(false, "Value expected", " =><,\r\n"));
          value = getFieldValueCountingParameters(fieldValue);
        }
      else
        value = EMPTY_VALUE;

      // INSERT FIELD NAME TO BE REMOVED
      removeEntries.add(new OPair<String, Object>(fieldName, value));
      parserSkipWhiteSpaces();
    }

    if (removeEntries.size() == 0)
      throwSyntaxErrorException("Field(s) to remove are missed. Example: name, salary");
  }

  private void parseIncrementFields() {
    String fieldName;
    String fieldValue;

    while (!parserIsEnded() && (incrementEntries.size() == 0 || parserGetLastSeparator() == ',')
        && !parserGetLastWord().equals(KEYWORD_WHERE)) {

      fieldName = parserRequiredWord(false, "Field name expected");
      parserRequiredKeyword("=");
      fieldValue = getBlock(parserRequiredWord(false, "Value expected"));

      // INSERT TRANSFORMED FIELD VALUE
      incrementEntries.put(fieldName, (Number) getFieldValueCountingParameters(fieldValue));
      parserSkipWhiteSpaces();
    }

    if (incrementEntries.size() == 0)
      throwSyntaxErrorException("Entries to increment <field> = <value> are missed. Example: salary = -100");
  }

    /**
     * Parses the returning keyword if found.
     */
    protected SQLUpdateReturnModeEnum parseReturn() throws OCommandSQLParsingException {
        parserNextWord(false," ");
        String returning = parserGetLastWord().trim();
        String optionalExp = "";

        if (returning.equalsIgnoreCase("COUNT"))
        {
            returnMode = SQLUpdateReturnModeEnum.COUNT;
            returnExpression = null;
        }else
        if (returning.equalsIgnoreCase("BEFORE") || returning.equalsIgnoreCase("AFTER"))
        {
            returnMode = (returning.equalsIgnoreCase("BEFORE"))? SQLUpdateReturnModeEnum.BEFORE : SQLUpdateReturnModeEnum.AFTER;
            parserNextWord(false," ");
            returning = parserGetLastWord().trim();
            if (returning.equalsIgnoreCase(KEYWORD_WHERE) || returning.equalsIgnoreCase(KEYWORD_TIMEOUT) || returning.equalsIgnoreCase(KEYWORD_LIMIT)
                    || returning.equalsIgnoreCase(KEYWORD_UPSERT) || returning.equalsIgnoreCase(KEYWORD_LOCK) || returning.length()==0)
            {
                returnExpression = null;
                parserGoBack();
            }
            else
            {
                if (returning.startsWith("$") || returning.startsWith("@"))
                    returnExpression = (returning.length()>0) ? OSQLHelper.parseValue(this, returning,this.getContext()) : null;
                else
                    throwSyntaxErrorException("record attribute (@attributes) or functions with $current variable expected");

            }
        }else
            throwSyntaxErrorException(" COUNT | BEFORE | AFTER keywords expected");


        return returnMode;
    }

}
