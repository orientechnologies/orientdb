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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.query.OQueryAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLSelect;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionRuntime;
import com.orientechnologies.orient.core.sql.functions.coll.OSQLFunctionDistinct;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;
import com.orientechnologies.orient.server.hazelcast.sharding.hazelcast.OHazelcastResultListener;
import com.orientechnologies.orient.server.hazelcast.sharding.hazelcast.ServerInstance;

/**
 * Executor for distributed select command and its result merge
 * 
 * @author edegtyarenko
 * @since 25.10.12 8:12
 */
public class ODistributedSelectQueryExecutor extends OAbstractDistributedQueryExecutor implements MessageListener<byte[]> {

  private static final int                        QUEUE_SIZE          = 100;
  private static final AtomicLong                 SELECT_ID_GENERATOR = new AtomicLong(0);

  private final long                              storageId;
  private final long                              selectId;

  private final boolean                           anyFunctionAggregate;
  private final List<OPair<String, OSQLFunction>> mergers             = new ArrayList<OPair<String, OSQLFunction>>();
  private OPair<String, OSQLFunctionDistinct>     distinct            = null;
  private List<OPair<String, String>>             order               = null;
  private final int                               limit;
  private final boolean                           async;

  private final OCommandResultListener            resultListener;

  private final BlockingQueue<byte[]>             plainResult         = new ArrayBlockingQueue<byte[]>(QUEUE_SIZE);

  private final ITopic<byte[]>                    resultTopic;

  public ODistributedSelectQueryExecutor(OCommandRequestText iCommand, OCommandExecutorSQLSelect executor,
      OStorageEmbedded wrapped, ServerInstance serverInstance) {
    super(iCommand, wrapped, serverInstance);

    this.selectId = SELECT_ID_GENERATOR.incrementAndGet();
    this.storageId = serverInstance.getLocalNode().getNodeId();

    this.anyFunctionAggregate = executor.isAnyFunctionAggregates();
    if (executor.getProjections() != null) {
      for (Map.Entry<String, Object> projection : executor.getProjections().entrySet()) {
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
    this.order = executor.getOrderedFields();
    this.limit = executor.getLimit();
    this.resultListener = (iCommand.getResultListener() != null && !(iCommand.getResultListener() instanceof OSQLSynchQuery)) ? iCommand
        .getResultListener() : null;
    this.async = resultListener != null && !anyFunctionAggregate && distinct == null && mergers.isEmpty() && order == null;

    this.resultTopic = ServerInstance.getHazelcast().getTopic(getResultTopicName(storageId, selectId));
    this.resultTopic.addMessageListener(this);
  }

  @Override
  protected void addResult(Object result) {
    // hear result is always null, no actions needed
  }

  @Override
  public void onMessage(Message<byte[]> message) {
    try {
      plainResult.put(message.getMessageObject());
    } catch (InterruptedException e) {
      OLogManager.instance().warn(this, "Failed to put message into queue");
    }
  }

  @Override
  public Object execute() {
    if (iCommand.getParameters().size() == 1) {
      final Map.Entry<Object, Object> entry = iCommand.getParameters().entrySet().iterator().next();
      if (entry.getKey().equals(Integer.valueOf(0)) && entry.getValue() == null) {
        iCommand.getParameters().clear();
      }
    }
    int remainingExecutors = runCommandOnAllNodes(new OSQLAsynchQuery(iCommand.getText(), iCommand.getLimit(),
        iCommand instanceof OQueryAbstract ? ((OQueryAbstract) iCommand).getFetchPlan() : null, iCommand.getParameters(),
        new OHazelcastResultListener(ServerInstance.getHazelcast(), storageId, selectId)));

    int processed = 0;
    final List<OIdentifiable> result = new ArrayList<OIdentifiable>();

    while (true) {
      try {
        final byte[] plainItem = plainResult.take();
        final Object item = OCommandResultSerializationHelper.readFromStream(plainItem);
        if (item instanceof OIdentifiable) {
          if (async) {
            resultListener.result(item);
          } else {
            result.add((OIdentifiable) item);
          }
          processed++;
        } else if (item instanceof OHazelcastResultListener.EndOfResult) {
          remainingExecutors--;
        } else {
          throw new IllegalArgumentException("Invalid type provided");
        }
      } catch (InterruptedException e) {
        OLogManager.instance().warn(this, "Failed to take message from queue");
      } catch (IOException e) {
        OLogManager.instance().warn(this, "Error deserializing result");
      }

      if (remainingExecutors <= failedNodes.get() || (async && limit != -1 && processed >= limit)) {
        break;
      }
    }

    resultTopic.destroy();

    if (async) {
      return null;
    } else {
      return processResult(result);
    }
  }

  private List<OIdentifiable> processResult(List<OIdentifiable> result) {
    final Map<String, Object> values = new HashMap<String, Object>();
    for (OPair<String, OSQLFunction> merger : mergers) {
      final List<Object> dataToMerge = new ArrayList<Object>();
      for (OIdentifiable o : result) {
        dataToMerge.add(((ODocument) o).field(merger.getKey()));
      }
      values.put(merger.getKey(), merger.getValue().mergeDistributedResult(dataToMerge));
    }
    if (distinct != null) {
      final List<OIdentifiable> resultToMerge = new ArrayList<OIdentifiable>(result);
      result.clear();
      for (OIdentifiable record : resultToMerge) {
        Object ret = distinct.getValue()
            .execute(record, null, new Object[] { ((ODocument) record).field(distinct.getKey()) }, null);
        if (ret != null) {
          final ODocument resultItem = new ODocument().setOrdered(true); // ASSIGN A TEMPORARY RID TO ALLOW PAGINATION IF ANY
          ((ORecordId) resultItem.getIdentity()).clusterId = -2;
          resultItem.field(distinct.getKey(), ret);
          result.add(resultItem);
        }
      }
    }
    if (anyFunctionAggregate && !result.isEmpty()) {
      // left only one result
      final OIdentifiable doc = result.get(0);
      result.clear();
      result.add(doc);
    }
    // inject values
    if (!values.isEmpty()) {
      for (Map.Entry<String, Object> entry : values.entrySet()) {
        for (OIdentifiable item : result) {
          ((ODocument) item).field(entry.getKey(), entry.getValue());
        }
      }
    }
    if (order != null) {
      ODocumentHelper.sort(result, order);
    }
    if (limit != -1 && result.size() > limit) {
      do {
        result.remove(result.size() - 1);
      } while (result.size() > limit);
    }
    if (!result.isEmpty() && result.get(0).getIdentity().getClusterId() == -2) {
      long position = 0;
      for (OIdentifiable id : result) {
        ((ORecordId) id.getIdentity()).clusterPosition = OClusterPositionFactory.INSTANCE.valueOf(position);
      }
    }
    if (resultListener != null) {
      for (Object o : result) {
        resultListener.result(o);
      }
    }
    return result;
  }

  public static String getResultTopicName(long storageId, long selectId) {
    return new StringBuilder("query-").append(storageId).append("-").append(selectId).toString();
  }
}
