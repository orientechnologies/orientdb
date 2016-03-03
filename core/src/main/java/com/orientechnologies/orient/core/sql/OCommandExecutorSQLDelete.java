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

import com.orientechnologies.common.parser.OStringParser;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.index.OCompositeIndexDefinition;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilter;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.parser.ODeleteStatement;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.core.sql.query.OSQLQuery;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
  private List<ORecord>        allDeletedRecords;

  private OSQLFilter           compiledFilter;
  private boolean              unsafe          = false;

  public OCommandExecutorSQLDelete() {
  }

  @SuppressWarnings("unchecked")
  public OCommandExecutorSQLDelete parse(final OCommandRequest iRequest) {
    final OCommandRequestText textRequest = (OCommandRequestText) iRequest;

    String queryText = textRequest.getText();
    String originalQuery = queryText;
    try {
      queryText = preParse(queryText, iRequest);
      textRequest.setText(queryText);
      final ODatabaseDocument database = getDatabase();

      init((OCommandRequestText) iRequest);

      query = null;
      recordCount = 0;

      if (parserTextUpperCase.endsWith(KEYWORD_UNSAFE)) {
        unsafe = true;
        parserText = parserText.substring(0, parserText.length() - KEYWORD_UNSAFE.length() - 1);
        parserTextUpperCase = parserTextUpperCase.substring(0, parserTextUpperCase.length() - KEYWORD_UNSAFE.length() - 1);
      }

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
            else if (word.equals(KEYWORD_UNSAFE))
              unsafe = true;
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
        query = database.command(new OSQLAsynchQuery<ODocument>("select from " + getSelectTarget(subjectName) + condition, this));
      }
    } finally {
      textRequest.setText(originalQuery);
    }

    return this;
  }

  private String getSelectTarget(String subjectName) {
    if(preParsedStatement == null){
      return subjectName;
    }
    return ((ODeleteStatement)preParsedStatement).fromClause.toString();
  }


  public Object execute(final Map<Object, Object> iArgs) {
    if (query == null && indexName == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    if (!returning.equalsIgnoreCase("COUNT"))
      allDeletedRecords = new ArrayList<ORecord>();

    if (query != null) {
      // AGAINST CLUSTERS AND CLASSES
      query.setContext(getContext());

      Object prevLockValue = query.getContext().getVariable("$locking");

      if (lockStrategy.equals("RECORD"))
        query.getContext().setVariable("$locking", OStorage.LOCKING_STRATEGY.EXCLUSIVE_LOCK);

      query.execute(iArgs);

      query.getContext().setVariable("$locking", prevLockValue);

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
          Map.Entry<Object, OIdentifiable> entry;

          while ((entry = cursor.nextEntry()) != null) {
            OIdentifiable rec = entry.getValue();
            rec = rec.getRecord();
            if (rec != null)
              allDeletedRecords.add((ORecord) rec);
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

  @Override
  public long getDistributedTimeout() {
    return OGlobalConfiguration.DISTRIBUTED_COMMAND_TASK_SYNCH_TIMEOUT.getValueAsLong();
  }

  /**
   * Deletes the current record.
   */
  public boolean result(final Object iRecord) {
    final ORecordAbstract record = ((OIdentifiable) iRecord).getRecord();

    try {
      if (record.getIdentity().isValid()) {
        if (returning.equalsIgnoreCase("BEFORE"))
          allDeletedRecords.add(record);

        // RESET VERSION TO DISABLE MVCC AVOIDING THE CONCURRENT EXCEPTION IF LOCAL CACHE IS NOT UPDATED
        record.getRecordVersion().disable();

        if (!unsafe && record instanceof ODocument) {
          // CHECK IF ARE VERTICES OR EDGES
          final OClass cls = ((ODocument) record).getSchemaClass();
          if (cls != null) {
            if (cls.isSubClassOf("V"))
              // FOUND VERTEX
              throw new OCommandExecutionException(
                  "'DELETE' command cannot delete vertices. Use 'DELETE VERTEX' command instead, or apply the 'UNSAFE' keyword to force it");
            else if (cls.isSubClassOf("E"))
              // FOUND EDGE
              throw new OCommandExecutionException(
                  "'DELETE' command cannot delete edges. Use 'DELETE EDGE' command instead, or apply the 'UNSAFE' keyword to force it");
          }
        }

        record.delete();

        recordCount++;
        return true;
      }
      return false;
    } finally {
      if (lockStrategy.equalsIgnoreCase("RECORD"))
        ((OAbstractPaginatedStorage) getDatabase().getStorage()).releaseWriteLock(record.getIdentity());
    }
  }

  public String getSyntax() {
    return "DELETE FROM <Class>|RID|cluster:<cluster> [UNSAFE] [LOCK <NONE|RECORD>] [RETURN <COUNT|BEFORE>] [WHERE <condition>*]";
  }

  @Override
  public void end() {
  }

  @Override
  public int getSecurityOperationType() {
    return ORole.PERMISSION_DELETE;
  }

  /**
   * Parses the returning keyword if found.
   */
  protected String parseReturn() throws OCommandSQLParsingException {
    final String returning = parserNextWord(true);

    if (!returning.equalsIgnoreCase("COUNT") && !returning.equalsIgnoreCase("BEFORE"))
      throwParsingException("Invalid " + KEYWORD_RETURN + " value set to '" + returning
          + "' but it should be COUNT (default), BEFORE. Example: " + KEYWORD_RETURN + " BEFORE");

    return returning;
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
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.WRITE;
  }

  @Override
  public OCommandDistributedReplicateRequest.DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return (indexName != null || query != null) && !getDatabase().getTransaction().isActive() ? DISTRIBUTED_EXECUTION_MODE.REPLICATE
        : DISTRIBUTED_EXECUTION_MODE.LOCAL;
  }

  public DISTRIBUTED_RESULT_MGMT getDistributedResultManagement() {
    return getDistributedExecutionMode() == DISTRIBUTED_EXECUTION_MODE.LOCAL ? DISTRIBUTED_RESULT_MGMT.CHECK_FOR_EQUALS
        : DISTRIBUTED_RESULT_MGMT.MERGE;
  }
}
