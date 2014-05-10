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

import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.common.parser.OStringParser;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilter;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.core.sql.query.OSQLQuery;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;

import java.util.*;

/**
 * SQL UPDATE command.
 * 
 * @author Luca Garulli
 * 
 */
public class OCommandExecutorSQLDelete extends OCommandExecutorSQLAbstract implements OCommandDistributedReplicateRequest,
    OCommandResultListener {
  public static final String   NAME            = "DELETE FROM";
  public static final String   KEYWORD_DELETE  = "DELETE";
  private static final String  VALUE_NOT_FOUND = "_not_found_";

  private OSQLQuery<ODocument> query;
  private String               indexName       = null;
  private int                  recordCount     = 0;
  private String               lockStrategy    = "NONE";
  private String               returning       = "COUNT";
  private List<ORecord<?>>     allDeletedRecords;

  private OSQLFilter           compiledFilter;

  public OCommandExecutorSQLDelete() {
  }

  @SuppressWarnings("unchecked")
  public OCommandExecutorSQLDelete parse(final OCommandRequest iRequest) {
    final ODatabaseRecord database = getDatabase();

    init((OCommandRequestText) iRequest);

    query = null;
    recordCount = 0;

    parserRequiredKeyword(OCommandExecutorSQLDelete.KEYWORD_DELETE);
    parserRequiredKeyword(OCommandExecutorSQLDelete.KEYWORD_FROM);

    String subjectName = parserRequiredWord(false, "Syntax error", " =><,\r\n");
    if (subjectName == null)
      throwSyntaxErrorException("Invalid subject name. Expected cluster, class, index or sub-query");

    if (OStringParser.startsWithIgnoreCase(subjectName, OCommandExecutorSQLAbstract.INDEX_PREFIX)) {
      // INDEX
      indexName = subjectName.substring(OCommandExecutorSQLAbstract.INDEX_PREFIX.length());

      if (!parserIsEnded()) {
        while (!parserIsEnded()) {
          final String word = parserGetLastWord();

          if (word.equals(KEYWORD_LOCK))
            lockStrategy = parseLock();
          else if (word.equals(KEYWORD_RETURN))
            returning = parseReturn();
          else if (word.equalsIgnoreCase(KEYWORD_WHERE))
            compiledFilter = OSQLEngine.getInstance().parseCondition(parserText.substring(parserGetCurrentPosition()),
                getContext(), KEYWORD_WHERE);

          parserNextWord(true);
        }

      } else
        parserSetCurrentPosition(-1);

    } else if (subjectName.startsWith("(")) {
      subjectName = subjectName.trim();
      query = database.command(new OSQLAsynchQuery<ODocument>(subjectName.substring(1, subjectName.length() - 1), this));

    } else {
      parserNextWord(true);

      while (!parserIsEnded()) {
        final String word = parserGetLastWord();

        if (word.equals(KEYWORD_LOCK))
          lockStrategy = parseLock();
        else if (word.equals(KEYWORD_RETURN))
          returning = parseReturn();
        else {
          parserGoBack();
          break;
        }

        parserNextWord(true);
      }

      final String condition = parserGetCurrentPosition() > -1 ? " " + parserText.substring(parserGetCurrentPosition()) : "";
      query = database.command(new OSQLAsynchQuery<ODocument>("select from " + subjectName + condition, this));
    }

    return this;
  }

  public Object execute(final Map<Object, Object> iArgs) {
    if (query == null && indexName == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    if (!returning.equalsIgnoreCase("COUNT"))
      allDeletedRecords = new ArrayList<ORecord<?>>();

    if (query != null) {
      // AGAINST CLUSTERS AND CLASSES
      if (lockStrategy.equals("RECORD"))
        query.getContext().setVariable("$locking", OStorage.LOCKING_STRATEGY.KEEP_EXCLUSIVE_LOCK);

      query.execute(iArgs);

      if (returning.equalsIgnoreCase("COUNT"))
        // RETURNS ONLY THE COUNT
        return recordCount;
      else
        // RETURNS ALL THE DELETED RECORDS
        return allDeletedRecords;

    } else {
      // AGAINST INDEXES
      if (compiledFilter != null)
        compiledFilter.bindParameters(iArgs);

      final OIndex index = getDatabase().getMetadata().getIndexManager().getIndex(indexName);
      if (index == null)
        throw new OCommandExecutionException("Target index '" + indexName + "' not found");

      Object key = null;
      Object value = VALUE_NOT_FOUND;

      if (compiledFilter == null || compiledFilter.getRootCondition() == null) {
        if (returning.equalsIgnoreCase("COUNT")) {
          // RETURNS ONLY THE COUNT
          final long total = index.getSize();
          index.clear();
          return total;
        } else {
          // RETURNS ALL THE DELETED RECORDS
          OIndexCursor cursor = index.cursor();
          Map.Entry<Object, OIdentifiable> entry = cursor.nextEntry();

          while (entry != null) {
            OIdentifiable rec = entry.getValue();
            rec = rec.getRecord();
            if (rec != null)
              allDeletedRecords.add((ORecord<?>) rec);
          }

          index.clear();

          return allDeletedRecords;
        }

      } else {
        if (KEYWORD_KEY.equalsIgnoreCase(compiledFilter.getRootCondition().getLeft().toString()))
          // FOUND KEY ONLY
          key = getIndexKey(index.getDefinition(), compiledFilter.getRootCondition().getRight());

        else if (KEYWORD_RID.equalsIgnoreCase(compiledFilter.getRootCondition().getLeft().toString())) {
          // BY RID
          value = OSQLHelper.getValue(compiledFilter.getRootCondition().getRight());

        } else if (compiledFilter.getRootCondition().getLeft() instanceof OSQLFilterCondition) {
          // KEY AND VALUE
          final OSQLFilterCondition leftCondition = (OSQLFilterCondition) compiledFilter.getRootCondition().getLeft();
          if (KEYWORD_KEY.equalsIgnoreCase(leftCondition.getLeft().toString()))
            key = getIndexKey(index.getDefinition(), leftCondition.getRight());

          final OSQLFilterCondition rightCondition = (OSQLFilterCondition) compiledFilter.getRootCondition().getRight();
          if (KEYWORD_RID.equalsIgnoreCase(rightCondition.getLeft().toString()))
            value = OSQLHelper.getValue(rightCondition.getRight());

        }

        final boolean result;
        if (value != VALUE_NOT_FOUND) {
          assert key != null;
          result = index.remove(key, (OIdentifiable) value);
        } else
          result = index.remove(key);

        if (returning.equalsIgnoreCase("COUNT"))
          return result ? 1 : 0;
        else
          // TODO: REFACTOR INDEX TO RETURN DELETED ITEMS
          throw new UnsupportedOperationException();
      }
    }
  }

  /**
   * Delete the current record.
   */
  public boolean result(final Object iRecord) {
    final ORecordAbstract<?> record = (ORecordAbstract<?>) iRecord;

    try {
      if (record.getIdentity().isValid()) {
        if (returning.equalsIgnoreCase("BEFORE"))
          allDeletedRecords.add(record);

        // RESET VERSION TO DISABLE MVCC AVOIDING THE CONCURRENT EXCEPTION IF LOCAL CACHE IS NOT UPDATED
        record.getRecordVersion().disable();
        record.delete();
        recordCount++;
        return true;
      }
      return false;
    } finally {
      if (lockStrategy.equalsIgnoreCase("RECORD"))
        ((OStorageEmbedded) getDatabase().getStorage()).releaseWriteLock(record.getIdentity());
    }
  }

  public boolean isReplicated() {
    return indexName != null;
  }

  public String getSyntax() {
    return "DELETE FROM <Class>|RID|cluster:<cluster> [LOCK <NONE|RECORD>] [RETURNING <COUNT|BEFORE>] [WHERE <condition>*]";
  }

  private Object getIndexKey(final OIndexDefinition indexDefinition, Object value) {
    if (indexDefinition instanceof OCompositeIndexDefinition) {
      if (value instanceof List) {
        final List<?> values = (List<?>) value;
        List<Object> keyParams = new ArrayList<Object>(values.size());

        for (Object o : values) {
          keyParams.add(OSQLHelper.getValue(o));
        }
        return indexDefinition.createValue(keyParams);
      } else {
        value = OSQLHelper.getValue(value);
        if (value instanceof OCompositeKey) {
          return value;
        } else {
          return indexDefinition.createValue(value);
        }
      }
    } else {
      return OSQLHelper.getValue(value);
    }
  }

    /**
     * Parses the returning keyword if found.
     */
    protected String parseReturn() throws OCommandSQLParsingException {
        parserNextWord(true);
        final String returning = parserGetLastWord();

        if (!returning.equalsIgnoreCase("COUNT") && !returning.equalsIgnoreCase("BEFORE"))
            throwParsingException("Invalid " + KEYWORD_RETURN + " value set to '" + returning
                    + "' but it should be COUNT (default), BEFORE. Example: " + KEYWORD_RETURN + " BEFORE");

        return returning;
    }



  @Override
  public void end() {
  }
}
