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
package com.orientechnologies.orient.server.hazelcast.sharding;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.sql.OAggregatorResultListener;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLDelegate;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLSelect;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionRuntime;
import com.orientechnologies.orient.core.sql.functions.coll.OSQLFunctionDistinct;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

/**
 * This class helps to merge result sets from several query executors
 * 
 * @author gman
 * @since 20.09.12 15:58
 */
public class ODistributedQueryHelper {

  private static final String                     RESULT_FIELD         = "result";
  private static final Set<Class>                 ALWAYS_DISTRIBUTABLE = new HashSet<Class>();

  private boolean                                 distributable        = false;
  private final boolean                           select;
  private boolean                                 anyFunctionAggregate = false;
  private final List<OPair<String, OSQLFunction>> mergers              = new ArrayList<OPair<String, OSQLFunction>>();
  private OPair<String, OSQLFunctionDistinct>     distinct             = null;
  private List<OPair<String, String>>             order                = null;
  private int                                     limit                = -1;
  private int                                     processed            = 0;
  private OCommandResultListener                  resultListener;
  private final OCommandRequestText               iCommand;
  private final List<OIdentifiable>               tempResult           = new ArrayList<OIdentifiable>();

  public ODistributedQueryHelper(OCommandRequestText iCommand, Set<Integer> undistributedClusters) {

    this.iCommand = iCommand;

    final OCommandExecutor executor = OCommandManager.instance().getExecutor(iCommand);
    executor.parse(iCommand);

    final OCommandExecutor realExecutor = executor instanceof OCommandExecutorSQLDelegate ? ((OCommandExecutorSQLDelegate) executor)
        .getDelegate() : executor;

    if (realExecutor instanceof OCommandExecutorSQLSelect) {
      final OCommandExecutorSQLSelect selectExecutor = (OCommandExecutorSQLSelect) realExecutor;

      select = true;
      distributable = true;

      for (Integer c : selectExecutor.getInvolvedClusters()) {
        if (undistributedClusters.contains(c)) {
          distributable = false;
          break;
        }
      }

      order = selectExecutor.getOrderedFields();
      anyFunctionAggregate = selectExecutor.isAnyFunctionAggregates();
      limit = selectExecutor.getLimit();
      if (selectExecutor.getProjections() != null) {
        for (Map.Entry<String, Object> projection : selectExecutor.getProjections().entrySet()) {
          if (projection.getValue() instanceof OSQLFunctionRuntime) {
            final OSQLFunctionRuntime fr = (OSQLFunctionRuntime) projection.getValue();
            if (fr.getFunction().shouldMergeDistributedResult()) {
              mergers.add(new OPair<String, OSQLFunction>(projection.getKey(), fr.getFunction()));
            } else if (fr.getFunction() instanceof OSQLFunctionDistinct) {
              distinct = new OPair<String, OSQLFunctionDistinct>(projection.getKey(), (OSQLFunctionDistinct) fr.getFunction());
            }
          }
        }
      }
      if (iCommand.getResultListener() != null && !(iCommand.getResultListener() instanceof OSQLSynchQuery)) {
        resultListener = iCommand.getResultListener();
      } else {
        resultListener = null;
      }
    } else {
      select = false;
      distributable = ALWAYS_DISTRIBUTABLE.contains(realExecutor.getClass());
    }
  }

  public OCommandRequestText getPreparedRemoteCommand() {
    if (!distributable) {
      throw new IllegalStateException("Non-distributable command");
    }
    if (select) {
      return new OSQLAsynchQuery(iCommand.getText(), new OAggregatorResultListener());
    } else {
      return iCommand;
    }
  }

  public boolean isDistributable() {
    return distributable;
  }

  public void addToResult(Object result) {
    final Collection<OIdentifiable> resultAsCollection = resultAsCollection(result);
    synchronized (tempResult) {
      tempResult.addAll(resultAsCollection);
      processed += resultAsCollection.size();
    }
  }

  private Collection<OIdentifiable> resultAsCollection(Object result) {
    if (result == null) {
      return Collections.emptySet();
    } else if (result instanceof OIdentifiable) {
      return Collections.singleton((OIdentifiable) result);
    } else if (result instanceof Collection<?>) {
      return (List<OIdentifiable>) result;
    } else if (result instanceof Integer) {
      // insert/update/delete result
      final ODocument wrapper = new ODocument();
      wrapper.field(RESULT_FIELD, result, OType.INTEGER);
      return Collections.<OIdentifiable> singleton(wrapper);
    } else {
      throw new IllegalArgumentException("Invalid result type");
    }
  }

  public Object getResult() {
    if (select) {
      final Map<String, Object> values = new HashMap<String, Object>();
      for (OPair<String, OSQLFunction> merger : mergers) {
        final List<Object> dataToMerge = new ArrayList<Object>();
        for (OIdentifiable o : tempResult) {
          dataToMerge.add(((ODocument) o).field(merger.getKey()));
        }
        values.put(merger.getKey(), merger.getValue().mergeDistributedResult(dataToMerge));
      }
      if (distinct != null) {
        final List<OIdentifiable> resultToMerge = new ArrayList<OIdentifiable>(tempResult);
        tempResult.clear();
        for (OIdentifiable record : resultToMerge) {
          Object ret = distinct.getValue().execute(record, new Object[] { ((ODocument) record).field(distinct.getKey()) }, null);
          if (ret != null) {
            final ODocument result = new ODocument().setOrdered(true); // ASSIGN A TEMPORARY RID TO ALLOW PAGINATION IF ANY
            result.field(distinct.getKey(), ret);
            tempResult.add(result);
          }
        }
      }
      if (anyFunctionAggregate && !tempResult.isEmpty()) {
        // left only one result
        final OIdentifiable doc = tempResult.get(0);
        tempResult.clear();
        tempResult.add(doc);
      }
      // inject values
      if (!values.isEmpty()) {
        for (Map.Entry<String, Object> entry : values.entrySet()) {
          for (OIdentifiable item : tempResult) {
            ((ODocument) item).field(entry.getKey(), entry.getValue());
          }
        }
      }
      if (order != null) {
        ODocumentHelper.sort(tempResult, order);
      }
      if (!tempResult.isEmpty() && tempResult.get(0).getIdentity().getClusterId() == -2) {
        processed = 0;
        for (OIdentifiable id : tempResult) {
          ((ORecordId) id.getIdentity()).clusterPosition = processed++;
        }
      }
      if (limit != -1 && tempResult.size() > limit) {
        do {
          tempResult.remove(tempResult.size() - 1);
        } while (tempResult.size() > limit);
      }
      if (resultListener != null) {
        for (Object o : tempResult) {
          resultListener.result(o);
        }
      }
      return tempResult;
    } else {
      int result = 0;
      for (OIdentifiable obj : tempResult) {
        result += ((ODocument) obj).<Integer> field(RESULT_FIELD);
      }
      return result;
    }
  }

  public boolean asyncMode() {
    return resultListener != null && !anyFunctionAggregate && order == null && distinct == null && mergers.isEmpty();
  }
}
