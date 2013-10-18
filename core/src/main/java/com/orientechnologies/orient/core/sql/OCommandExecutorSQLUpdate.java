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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.filter.OSQLFilter;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItem;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionRuntime;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;

/**
 * SQL UPDATE command.
 * 
 * @author Luca Garulli
 * 
 */
public class OCommandExecutorSQLUpdate extends OCommandExecutorSQLSetAware implements OCommandResultListener {
  public static final String                 KEYWORD_UPDATE    = "UPDATE";
  private static final String                KEYWORD_ADD       = "ADD";
  private static final String                KEYWORD_PUT       = "PUT";
  private static final String                KEYWORD_REMOVE    = "REMOVE";
  private static final String                KEYWORD_INCREMENT = "INCREMENT";
  private static final String                KEYWORD_MERGE     = "MERGE";

  private Map<String, Object>                setEntries        = new LinkedHashMap<String, Object>();
  private List<OPair<String, Object>>        addEntries        = new ArrayList<OPair<String, Object>>();
  private Map<String, OPair<String, Object>> putEntries        = new LinkedHashMap<String, OPair<String, Object>>();
  private List<OPair<String, Object>>        removeEntries     = new ArrayList<OPair<String, Object>>();
  private Map<String, Number>                incrementEntries  = new LinkedHashMap<String, Number>();
  private ODocument                          merge             = null;

  private OQuery<?>                          query;
  private OSQLFilter                         compiledFilter;
  private int                                recordCount       = 0;
  private String                             subjectName;
  private static final Object                EMPTY_VALUE       = new Object();
  private OCommandParameters                 parameters;

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
            && !word.equals(KEYWORD_INCREMENT) && !word.equals(KEYWORD_CONTENT) && !word.equals(KEYWORD_MERGE)))
      throwSyntaxErrorException("Expected keyword " + KEYWORD_SET + "," + KEYWORD_ADD + "," + KEYWORD_CONTENT + "," + KEYWORD_MERGE
          + "," + KEYWORD_PUT + "," + KEYWORD_REMOVE + " or " + KEYWORD_INCREMENT);

    while (!parserIsEnded() && !parserGetLastWord().equals(OCommandExecutorSQLAbstract.KEYWORD_WHERE)) {
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
        || additionalStatement.equals(OCommandExecutorSQLAbstract.KEYWORD_LIMIT))
      query = new OSQLAsynchQuery<ODocument>("select from " + subjectName + " " + additionalStatement + " "
          + parserText.substring(parserGetCurrentPosition()), this);
    else if (additionalStatement != null && !additionalStatement.isEmpty())
      throwSyntaxErrorException("Invalid keyword " + additionalStatement);
    else
      query = new OSQLAsynchQuery<ODocument>("select from " + subjectName, this);

    return this;
  }

  public Object execute(final Map<Object, Object> iArgs) {
    if (subjectName == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    parameters = new OCommandParameters(iArgs);

    Map<Object, Object> queryArgs = new HashMap<Object, Object>();
    for (int i = parameterCounter; i < parameters.size(); i++) {
      if (parameters.getByName(i) != null)
        queryArgs.put(i - parameterCounter, parameters.getByName(i));
    }

    query.setUseCache(false);
    query.setContext(context);
    getDatabase().query(query, queryArgs);
    return recordCount;
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

    if (content != null) {
      // REPLACE ALL THE CONTENT
      record.clear();
      record.merge(content, false, false);
      updatedRecords.add(record);
    }

    if (merge != null) {
      // REPLACE ALL THE CONTENT
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
    for (Map.Entry<String, Number> entry : incrementEntries.entrySet()) {
      final Number prevValue = record.field(entry.getKey());

      if (prevValue == null)
        // NO PREVIOUS VALUE: CONSIDER AS 0
        record.field(entry.getKey(), entry.getValue());
      else
        // COMPUTING INCREMENT
        record.field(entry.getKey(), OType.increment(prevValue, entry.getValue()));

      updatedRecords.add(record);
    }

    Object v;

    // BIND VALUES TO ADD
    Collection<Object> coll;
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
        }

        if (coll == null)
          // IN ALL OTHER CASES USE A LIST
          coll = new ArrayList<Object>();

        record.field(entry.getKey(), coll);
      } else {
        fieldValue = record.field(entry.getKey());

        if (fieldValue instanceof Collection<?>)
          coll = (Collection<Object>) fieldValue;
        else
          continue;
      }

      v = entry.getValue();

      if (v instanceof OSQLFilterItem)
        v = ((OSQLFilterItem) v).getValue(record, context);
      else if (v instanceof OSQLFunctionRuntime)
        v = ((OSQLFunctionRuntime) v).execute(record, null, context);

      coll.add(v);
      updatedRecords.add(record);
    }

    // BIND VALUES TO PUT (AS MAP)
    Map<String, Object> map;
    OPair<String, Object> pair;
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
          v = ((OSQLFilterItem) v).getValue(record, context);
        else if (pair.getValue() instanceof OSQLFunctionRuntime)
          v = ((OSQLFunctionRuntime) v).execute(record, null, context);

        map.put(pair.getKey(), v);
        updatedRecords.add(record);
      }
    }

    // REMOVE FIELD IF ANY
    for (OPair<String, Object> entry : removeEntries) {
      v = entry.getValue();
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

  protected void parseMerge() {
    if (!parserIsEnded() && !parserGetLastWord().equals(KEYWORD_WHERE)) {
      final String contentAsString = parserRequiredWord(false, "document to merge expected").trim();
      merge = new ODocument().fromJSON(contentAsString);
      parserSkipWhiteSpaces();
    }

    if (merge == null)
      throwSyntaxErrorException("Document to merge not provided. Example: MERGE { \"name\": \"Jay\" }");
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
          fieldValue = getBlock(parserRequiredWord(false, "Value expected"));
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

  @Override
  public String getSyntax() {
    return "UPDATE <class>|cluster:<cluster>> [SET|ADD|PUT|REMOVE|INCREMENT|CONTENT {<JSON>}|MERGE {<JSON>}] [[,] <field-name> = <expression>|<sub-command>]* [WHERE <conditions>]";
  }

  @Override
  public void end() {
  }

  protected String getBlock(String fieldValue) {
    if (fieldValue.startsWith("{") || fieldValue.startsWith("[") || fieldValue.startsWith("[")) {
      parserSkipWhiteSpaces();
      final StringBuilder buffer = new StringBuilder();
      parserSetCurrentPosition(OStringSerializerHelper.parse(parserText, buffer, parserGetCurrentPosition(), -1,
          OStringSerializerHelper.DEFAULT_FIELD_SEPARATOR, true, true, false, OStringSerializerHelper.DEFAULT_IGNORE_CHARS));
      fieldValue = buffer.toString();
    }
    return fieldValue;
  }

}
