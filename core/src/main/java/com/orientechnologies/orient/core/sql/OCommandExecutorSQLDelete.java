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
import java.util.List;
import java.util.Map;

import com.orientechnologies.common.collection.OCompositeKey;
import com.orientechnologies.common.parser.OStringParser;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.index.OCompositeIndexDefinition;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilter;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.core.sql.query.OSQLQuery;

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
        parserNextWord(true);

        if (parserGetLastWord().equalsIgnoreCase(KEYWORD_WHERE))
          compiledFilter = OSQLEngine.getInstance().parseCondition(parserText.substring(parserGetCurrentPosition()), getContext(),
              KEYWORD_WHERE);
      } else
        parserSetCurrentPosition(-1);

    } else if (subjectName.startsWith("(")) {
      subjectName = subjectName.trim();
      query = database.command(new OSQLAsynchQuery<ODocument>(subjectName.substring(1, subjectName.length() - 1), this));

    } else {
      final String condition = parserGetCurrentPosition() > -1 ? " " + parserText.substring(parserGetCurrentPosition()) : "";
      query = database.command(new OSQLAsynchQuery<ODocument>("select from " + subjectName + condition, this));
    }

    return this;
  }

  public Object execute(final Map<Object, Object> iArgs) {
    if (query == null && indexName == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    if (query != null) {
      // AGAINST CLUSTERS AND CLASSES
      query.execute(iArgs);
      return recordCount;
    } else {
      // AGAINST INDEXES

      if (compiledFilter != null)
        compiledFilter.bindParameters(iArgs);

      final OIndex<?> index = getDatabase().getMetadata().getIndexManager().getIndex(indexName);
      if (index == null)
        throw new OCommandExecutionException("Target index '" + indexName + "' not found");

      Object key = null;
      Object value = VALUE_NOT_FOUND;

      if (compiledFilter == null || compiledFilter.getRootCondition() == null) {
        final long total = index.getSize();
        index.clear();
        return total;
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
        if (value != VALUE_NOT_FOUND)
          if (key != null)
            result = index.remove(key, (OIdentifiable) value);
          else
            return index.remove((OIdentifiable) value);
        else
          result = index.remove(key);

        return result ? 1 : 0;
      }
    }
  }

  /**
   * Delete the current record.
   */
  public boolean result(final Object iRecord) {
    final ORecordAbstract<?> record = (ORecordAbstract<?>) iRecord;

    if (record.getIdentity().isValid()) {
      // RESET VERSION TO DISABLE MVCC AVOIDING THE CONCURRENT EXCEPTION IF LOCAL CACHE IS NOT UPDATED
      record.getRecordVersion().disable();
      record.delete();
      recordCount++;
      return true;
    }
    return false;
  }

  public boolean isReplicated() {
    return indexName != null;
  }

  public String getSyntax() {
    return "DELETE FROM <Class>|RID|cluster:<cluster> [WHERE <condition>*]";
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

  @Override
  public void end() {
  }
}
