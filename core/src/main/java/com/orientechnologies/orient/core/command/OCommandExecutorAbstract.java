/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.command;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.parser.OBaseParser;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OExecutionThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandInterruptedException;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.sql.filter.OSQLPredicate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Abstract implementation of Executor Command interface.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
@SuppressWarnings("unchecked")
public abstract class OCommandExecutorAbstract extends OBaseParser implements OCommandExecutor {
  protected OProgressListener progressListener;
  protected int limit = -1;
  protected Map<Object, Object> parameters;
  protected OCommandContext context;

  public static ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.instance().get();
  }

  public OCommandExecutorAbstract init(final OCommandRequestText iRequest) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.COMMAND, ORole.PERMISSION_READ);
    parserText = iRequest.getText().trim();
    parserTextUpperCase = OSQLPredicate.upperCase(parserText);
    return this;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " [text=" + parserText + "]";
  }

  public OProgressListener getProgressListener() {
    return progressListener;
  }

  public <RET extends OCommandExecutor> RET setProgressListener(
      final OProgressListener progressListener) {
    this.progressListener = progressListener;
    return (RET) this;
  }

  public String getUndoCommand() {
    return null;
  }

  public long getDistributedTimeout() {
    return getDatabase()
        .getConfiguration()
        .getValueAsLong(OGlobalConfiguration.DISTRIBUTED_COMMAND_LONG_TASK_SYNCH_TIMEOUT);
  }

  public int getLimit() {
    return limit;
  }

  public <RET extends OCommandExecutor> RET setLimit(final int iLimit) {
    this.limit = iLimit;
    return (RET) this;
  }

  public Map<Object, Object> getParameters() {
    return parameters;
  }

  @Override
  public String getFetchPlan() {
    return null;
  }

  public OCommandContext getContext() {
    if (context == null) {
      context = new OBasicCommandContext(ODatabaseRecordThreadLocal.instance().getIfDefined());
    }
    return context;
  }

  public void setContext(final OCommandContext iContext) {
    context = iContext;
  }

  @Override
  public Set<String> getInvolvedClusters() {
    return Collections.EMPTY_SET;
  }

  @Override
  public int getSecurityOperationType() {
    return ORole.PERMISSION_READ;
  }

  public boolean involveSchema() {
    return false;
  }

  protected boolean checkInterruption() {
    return checkInterruption(this.context);
  }

  public static boolean checkInterruption(final OCommandContext iContext) {
    if (OExecutionThreadLocal.isInterruptCurrentOperation())
      throw new OCommandInterruptedException("The command has been interrupted");

    if (iContext != null && !iContext.checkTimeout()) return false;

    return true;
  }

  public OCommandDistributedReplicateRequest.DISTRIBUTED_RESULT_MGMT
      getDistributedResultManagement() {
    return OCommandDistributedReplicateRequest.DISTRIBUTED_RESULT_MGMT.CHECK_FOR_EQUALS;
  }

  @Override
  public boolean isLocalExecution() {
    return false;
  }

  @Override
  public boolean isCacheable() {
    return false;
  }

  public Object mergeResults(final Map<String, Object> results) throws Exception {

    if (results.isEmpty()) return null;

    Object aggregatedResult = null;

    for (Map.Entry<String, Object> entry : results.entrySet()) {
      final String nodeName = entry.getKey();
      final Object nodeResult = entry.getValue();

      if (nodeResult instanceof Collection) {
        if (aggregatedResult == null) aggregatedResult = new ArrayList();

        ((List) aggregatedResult).addAll((Collection<?>) nodeResult);

      } else if (nodeResult instanceof Exception)

        // RECEIVED EXCEPTION
        throw (Exception) nodeResult;
      else if (nodeResult instanceof OIdentifiable) {
        if (aggregatedResult == null) aggregatedResult = new ArrayList();

        ((List) aggregatedResult).add(nodeResult);

      } else if (nodeResult instanceof Number) {
        if (aggregatedResult == null) aggregatedResult = nodeResult;
        else OMultiValue.add(aggregatedResult, nodeResult);
      }
    }

    return aggregatedResult;
  }

  public boolean isDistributedExecutingOnLocalNodeFirst() {
    return true;
  }
}
