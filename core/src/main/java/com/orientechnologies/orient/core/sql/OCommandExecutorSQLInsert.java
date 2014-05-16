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

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SQL INSERT command.
 * 
 * @author Luca Garulli
 * @author Johann Sorel (Geomatys)
 */
public class OCommandExecutorSQLInsert extends OCommandExecutorSQLSetAware implements OCommandDistributedReplicateRequest,
    OCommandResultListener {
  public static final String             KEYWORD_INSERT   = "INSERT";
  protected static final String          KEYWORD_RETURN   = "RETURN";
  private static final String            KEYWORD_VALUES   = "VALUES";
  private String                         className        = null;
  private String                         clusterName      = null;
  private String                         indexName        = null;
  private List<Map<String, Object>>      newRecords;
  private OSQLAsynchQuery<OIdentifiable> subQuery         = null;
  private AtomicLong                     saved            = new AtomicLong(0);
  private Object                         returnExpression = null;
  private List<ODocument>                queryResult      = null;

  @SuppressWarnings("unchecked")
  public OCommandExecutorSQLInsert parse(final OCommandRequest iRequest) {
    final ODatabaseRecord database = getDatabase();

    init((OCommandRequestText) iRequest);

    className = null;
    newRecords = null;
    content = null;

    parserRequiredKeyword("INSERT");
    parserRequiredKeyword("INTO");

    String subjectName = parserRequiredWord(true, "Invalid subject name. Expected cluster, class or index");
    if (subjectName.startsWith(OCommandExecutorSQLAbstract.CLUSTER_PREFIX))
      // CLUSTER
      clusterName = subjectName.substring(OCommandExecutorSQLAbstract.CLUSTER_PREFIX.length());

    else if (subjectName.startsWith(OCommandExecutorSQLAbstract.INDEX_PREFIX))
      // INDEX
      indexName = subjectName.substring(OCommandExecutorSQLAbstract.INDEX_PREFIX.length());

    else {
      // CLASS
      if (subjectName.startsWith(OCommandExecutorSQLAbstract.CLASS_PREFIX))
        subjectName = subjectName.substring(OCommandExecutorSQLAbstract.CLASS_PREFIX.length());

      final OClass cls = database.getMetadata().getSchema().getClass(subjectName);
      if (cls == null)
        throwParsingException("Class " + subjectName + " not found in database");

      className = cls.getName();
    }

    parserSkipWhiteSpaces();
    if (parserIsEnded())
      throwSyntaxErrorException("Set of fields is missed. Example: (name, surname) or SET name = 'Bill'");

    final String temp = parseOptionalWord(true);
    if (temp.equals("CLUSTER")) {
      clusterName = parserRequiredWord(false);

      parserSkipWhiteSpaces();
      if (parserIsEnded())
        throwSyntaxErrorException("Set of fields is missed. Example: (name, surname) or SET name = 'Bill'");
    } else
      parserGoBack();

    newRecords = new ArrayList<Map<String, Object>>();
    Boolean sourceClauseProcessed = false;
    if (parserGetCurrentChar() == '(') {
      parseValues();
      parserNextWord(true, " \r\n");
      sourceClauseProcessed = true;
    } else {
      parserNextWord(true, " ,\r\n");

      if (parserGetLastWord().equals(KEYWORD_CONTENT)) {
        newRecords = null;
        parseContent();
        sourceClauseProcessed = true;
      } else if (parserGetLastWord().equals(KEYWORD_SET)) {
        final LinkedHashMap<String, Object> fields = new LinkedHashMap<String, Object>();
        newRecords.add(fields);
        parseSetFields(fields);
        sourceClauseProcessed = true;
      }
    }
    if (sourceClauseProcessed)
      parserNextWord(true, " \r\n");
    // it has to be processed before KEYWORD_FROM in order to not be taken as part of SELECT
    if (parserGetLastWord().equals(KEYWORD_RETURN)) {
      parseReturn(!sourceClauseProcessed);
      parserNextWord(true, " \r\n");
    }

    if (!sourceClauseProcessed) {
      if (parserGetLastWord().equals(KEYWORD_FROM)) {
        newRecords = null;
        subQuery = new OSQLAsynchQuery<OIdentifiable>(parserText.substring(parserGetCurrentPosition()), this);
      }
    }

    return this;
  }

  /**
   * Execute the INSERT and return the ODocument object created.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (newRecords == null && content == null && subQuery == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    final OCommandParameters commandParameters = new OCommandParameters(iArgs);
    if (indexName != null) {
      if (newRecords == null)
        throw new OCommandExecutionException("No key/value found");

      final OIndex<?> index = getDatabase().getMetadata().getIndexManager().getIndex(indexName);
      if (index == null)
        throw new OCommandExecutionException("Target index '" + indexName + "' not found");

      // BIND VALUES
      Map<String, Object> result = null;

      for (Map<String, Object> candidate : newRecords) {
        index.put(getIndexKeyValue(commandParameters, candidate), getIndexValue(commandParameters, candidate));
        result = candidate;
      }

      // RETURN LAST ENTRY
      return prepareReturnItem(new ODocument(result));
    } else {

      // CREATE NEW DOCUMENTS
      final List<ODocument> docs = new ArrayList<ODocument>();
      if (newRecords != null) {
        for (Map<String, Object> candidate : newRecords) {
          final ODocument doc = className != null ? new ODocument(className) : new ODocument();
          OSQLHelper.bindParameters(doc, candidate, commandParameters, context);

          saveRecord(doc);
          docs.add(doc);
        }

        if (docs.size() == 1)
          return prepareReturnItem(docs.get(0));
        else
          return prepareReturnResult(docs);
      } else if (content != null) {
        final ODocument doc = className != null ? new ODocument(className) : new ODocument();
        doc.merge(content, true, false);
        saveRecord(doc);
        return prepareReturnItem(doc);
      } else if (subQuery != null) {
        subQuery.execute();
        if (queryResult != null)
          return prepareReturnResult(queryResult);

        return saved.longValue();
      }
    }
    return null;
  }

  public boolean isReplicated() {
    return indexName != null;
  }

  @Override
  public String getSyntax() {
    return "INSERT INTO [class:]<class>|cluster:<cluster>|index:<index> [(<field>[,]*) VALUES (<expression>[,]*)[,]*]|[SET <field> = <expression>|<sub-command>[,]*]|CONTENT {<JSON>} [RETURN <expression>] [FROM select-query]";
  }

  @Override
  public boolean result(final Object iRecord) {
    final ORecord<?> rec = ((OIdentifiable) iRecord).getRecord().copy();

    // RESET THE IDENTITY TO AVOID UPDATE
    rec.getIdentity().reset();

    if (rec instanceof ODocument && className != null)
      ((ODocument) rec).setClassName(className);

    rec.setDirty();
    synchronized (this) {
      saveRecord(rec);
      if (queryResult != null)
        queryResult.add(((ODocument) rec));
    }

    return true;
  }

  @Override
  public void end() {

  }

  protected Object prepareReturnResult(List<ODocument> res) {
    if (returnExpression == null)
      return res;// No transformation
    final ArrayList<Object> ret = new ArrayList<Object>();
    for (ODocument resItem : (List<ODocument>) res)
      ret.add(prepareReturnItem(resItem));
    return ret;
  }

  protected Object prepareReturnItem(ODocument item) {
    if (returnExpression == null)
      return item;// No transformation

    this.getContext().setVariable("current", item);
    final Object res = OSQLHelper.getValue(returnExpression, item, this.getContext());
    if (res instanceof OIdentifiable)
      return res;
    else {// wrapping doc
      final ODocument wrappingDoc = new ODocument("result", res);
      wrappingDoc.field("rid", item.getIdentity());// passing record id.In many cases usable on client side
      wrappingDoc.field("version", item.getVersion());// passing record version
      return wrappingDoc;
    }
  }

  protected void saveRecord(final ORecord<?> rec) {
    if (clusterName != null)
      rec.save(clusterName);
    else
      rec.save();
    saved.incrementAndGet();
  }

  protected void parseValues() {
    final int beginFields = parserGetCurrentPosition();

    final int endFields = parserText.indexOf(')', beginFields + 1);
    if (endFields == -1)
      throwSyntaxErrorException("Missed closed brace");

    final ArrayList<String> fieldNames = new ArrayList<String>();
    parserSetCurrentPosition(OStringSerializerHelper.getParameters(parserText, beginFields, endFields, fieldNames));
    if (fieldNames.size() == 0)
      throwSyntaxErrorException("Set of fields is empty. Example: (name, surname)");

    // REMOVE QUOTATION MARKS IF ANY
    for (int i = 0; i < fieldNames.size(); ++i)
      fieldNames.set(i, OStringSerializerHelper.removeQuotationMarks(fieldNames.get(i)));

    parserRequiredKeyword(KEYWORD_VALUES);
    parserSkipWhiteSpaces();
    if (parserIsEnded() || parserText.charAt(parserGetCurrentPosition()) != '(') {
      throwParsingException("Set of values is missed. Example: ('Bill', 'Stuart', 300)");
    }

    int blockStart = parserGetCurrentPosition();
    int blockEnd = parserGetCurrentPosition();

    final List<String> records = OStringSerializerHelper.smartSplit(parserText, new char[] { ',' }, blockStart, -1, true, true,
        false, false);
    for (String record : records) {

      final List<String> values = new ArrayList<String>();
      blockEnd += OStringSerializerHelper.getParameters(record, 0, -1, values);

      if (blockEnd == -1)
        throw new OCommandSQLParsingException("Missed closed brace. Use " + getSyntax(), parserText, blockStart);

      if (values.isEmpty())
        throw new OCommandSQLParsingException("Set of values is empty. Example: ('Bill', 'Stuart', 300). Use " + getSyntax(),
            parserText, blockStart);

      if (values.size() != fieldNames.size())
        throw new OCommandSQLParsingException("Fields not match with values", parserText, blockStart);

      // TRANSFORM FIELD VALUES
      final Map<String, Object> fields = new LinkedHashMap<String, Object>();
      for (int i = 0; i < values.size(); ++i)
        fields.put(fieldNames.get(i), OSQLHelper.parseValue(this, OStringSerializerHelper.decode(values.get(i).trim()), context));

      newRecords.add(fields);
      blockStart = blockEnd;
    }

  }

  /**
   * Parses the returning keyword if found.
   */
  protected void parseReturn(Boolean subQueryExpected) throws OCommandSQLParsingException {
    parserNextWord(false, " ");
    String returning = parserGetLastWord().trim();
    if (returning.startsWith("$") || returning.startsWith("@")) {
      if (subQueryExpected)
        queryResult = new ArrayList<ODocument>();
      returnExpression = (returning.length() > 0) ? OSQLHelper.parseValue(this, returning, this.getContext()) : null;
    } else
      throwSyntaxErrorException("record attribute (@attributes) or functions with $current variable expected");

  }

  private Object getIndexKeyValue(OCommandParameters commandParameters, Map<String, Object> candidate) {
    final Object parsedKey = candidate.get(KEYWORD_KEY);
    if (parsedKey instanceof OSQLFilterItemField) {
      final OSQLFilterItemField f = (OSQLFilterItemField) parsedKey;
      if (f.getRoot().equals("?"))
        // POSITIONAL PARAMETER
        return commandParameters.getNext();
      else if (f.getRoot().startsWith(":"))
        // NAMED PARAMETER
        return commandParameters.getByName(f.getRoot().substring(1));
    }
    return parsedKey;
  }

  private OIdentifiable getIndexValue(OCommandParameters commandParameters, Map<String, Object> candidate) {
    final Object parsedRid = candidate.get(KEYWORD_RID);
    if (parsedRid instanceof OSQLFilterItemField) {
      final OSQLFilterItemField f = (OSQLFilterItemField) parsedRid;
      if (f.getRoot().equals("?"))
        // POSITIONAL PARAMETER
        return (OIdentifiable) commandParameters.getNext();
      else if (f.getRoot().startsWith(":"))
        // NAMED PARAMETER
        return (OIdentifiable) commandParameters.getByName(f.getRoot().substring(1));
    }
    return (OIdentifiable) parsedRid;
  }

}
