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
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLDelegate;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLSelect;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionRuntime;
import com.orientechnologies.orient.core.sql.functions.coll.OSQLFunctionDistinct;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;

/**
 * This class helps to merge result sets from several query executors
 * 
 * @author gman
 * @since 20.09.12 15:58
 */
public class ODistributedQueryHelper {

  private boolean                                 distributable        = false;
  private boolean                                 anyFunctionAggregate = false;
  private final List<OPair<String, OSQLFunction>> mergers              = new ArrayList<OPair<String, OSQLFunction>>();
  private OPair<String, OSQLFunctionDistinct>     distinct             = null;
  private List<OPair<String, String>>             order                = null;
  private final OCommandResultListener            asyncResultListener;

  private final OCommandRequestText               iCommand;
  private final List<OIdentifiable>               tempResult           = new ArrayList<OIdentifiable>();

  public ODistributedQueryHelper(OCommandRequestText iCommand, Set<Integer> undistributedClusters) {

    this.iCommand = iCommand;

    final OCommandExecutor executor = OCommandManager.instance().getExecutor(iCommand);
    executor.parse(iCommand);

    final OCommandExecutor realExecutor = executor instanceof OCommandExecutorSQLDelegate ? ((OCommandExecutorSQLDelegate) executor)
        .getDelegate() : executor;

    // if ((realExecutor instanceof OCommandDistributedConditionalReplicateRequest &&
    // ((OCommandDistributedConditionalReplicateRequest) realExecutor).isReplicated()) ||
    // realExecutor instanceof OCommandDistributedReplicateRequest) {
    // distributable = true;
    // }

    if (realExecutor instanceof OCommandExecutorSQLSelect) {
      final OCommandExecutorSQLSelect selectExecutor = (OCommandExecutorSQLSelect) realExecutor;

      distributable = true;

      for (Integer c : selectExecutor.getInvolvedClusters()) {
        if (undistributedClusters.contains(c)) {
          distributable = false;
          break;
        }
      }

      order = selectExecutor.getOrderedFields();
      anyFunctionAggregate = selectExecutor.isAnyFunctionAggregates();

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

    }

    if (iCommand.getResultListener() != null
        && iCommand.getResultListener().getClass().equals(ONetworkProtocolBinary.AsyncResultListener.class)) {
      asyncResultListener = iCommand.getResultListener();
    } else {
      asyncResultListener = null;
    }
  }

  public OCommandRequestText getPreparedRemoteCommand() {
    if (!distributable) {
      throw new IllegalStateException("Non-distributable command");
    }
    if (asyncResultListener != null && iCommand instanceof OSQLSynchQuery) {
      iCommand.setResultListener((OSQLSynchQuery) iCommand);
    }
    return iCommand;
  }

  public boolean isDistributable() {
    return distributable;
  }

  public void addToResult(Object result) {
    if (asyncMode()) {
      for (Object o : resultAsCollection(result)) {
        asyncResultListener.result(o);
      }
    } else {
      tempResult.addAll(resultAsCollection(result));
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
      wrapper.field("result", result, OType.INTEGER);
      return Collections.<OIdentifiable> singleton(wrapper);
    } else {
      throw new IllegalArgumentException("Invalid result type");
    }
  }

  public Object getResult() {
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
      long pos = 0;
      for (OIdentifiable id : tempResult) {
        ((ORecordId) id.getIdentity()).clusterPosition = pos++;
      }
    }
    if (asyncResultListener != null) {
      for (Object o : tempResult) {
        asyncResultListener.result(o);
      }
    }
    return tempResult;
  }

  public boolean asyncMode() {
    return asyncResultListener != null && !anyFunctionAggregate && order == null && distinct == null && mergers.isEmpty();
  }
}
