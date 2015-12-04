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
package com.orientechnologies.orient.core.command;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.command.OCommandContext.TIMEOUT_STRATEGY;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.replication.OAsyncReplicationError;
import com.orientechnologies.orient.core.replication.OAsyncReplicationOk;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Text based Command Request abstract class.
 *
 * @author Luca Garulli
 */
@SuppressWarnings("serial")
public abstract class OCommandRequestAbstract implements OCommandRequestInternal, ODistributedCommand {
  protected OCommandResultListener resultListener;
  protected OProgressListener      progressListener;
  protected int              limit           = -1;
  protected long             timeoutMs       = OGlobalConfiguration.COMMAND_TIMEOUT.getValueAsLong();
  protected TIMEOUT_STRATEGY timeoutStrategy = TIMEOUT_STRATEGY.EXCEPTION;
  protected Map<Object, Object> parameters;
  protected String  fetchPlan = null;
  protected boolean useCache  = true;
  protected OCommandContext        context;
  protected OAsyncReplicationOk    onAsyncReplicationOk;
  protected OAsyncReplicationError onAsyncReplicationError;


  private final Set<String> nodesToExclude = new HashSet<String>();

  protected OCommandRequestAbstract() {
  }

  public OCommandResultListener getResultListener() {
    return resultListener;
  }

  public void setResultListener(OCommandResultListener iListener) {
    resultListener = iListener;
  }

  public Map<Object, Object> getParameters() {
    return parameters;
  }

  protected void setParameters(final Object... iArgs) {
    if (iArgs != null && iArgs.length>0)
      parameters = convertToParameters(iArgs);
  }

  @SuppressWarnings("unchecked")
  protected Map<Object, Object> convertToParameters(final Object... iArgs) {
    final Map<Object, Object> params;

    if (iArgs.length == 1 && iArgs[0] instanceof Map) {
      params = (Map<Object, Object>) iArgs[0];
    } else {
      params = new HashMap<Object, Object>(iArgs.length);
      for (int i = 0; i<iArgs.length; ++i) {
        Object par = iArgs[i];

        if (par instanceof OIdentifiable && ((OIdentifiable) par).getIdentity().isValid())
          // USE THE RID ONLY
          par = ((OIdentifiable) par).getIdentity();

        params.put(i, par);
      }
    }
    return params;
  }

  /**
   * Defines a callback to call in case of the asynchronous replication succeed.
   */
  @Override
  public OCommandRequestAbstract onAsyncReplicationOk(final OAsyncReplicationOk iCallback) {
    onAsyncReplicationOk = iCallback;
    return this;
  }


  /**
   * Defines a callback to call in case of error during the asynchronous replication.
   */
  @Override
  public OCommandRequestAbstract onAsyncReplicationError(final OAsyncReplicationError iCallback) {
    if (iCallback != null) {
      onAsyncReplicationError = new OAsyncReplicationError() {
        int retry = 0;

        @Override
        public ACTION onAsyncReplicationError(Throwable iException, final int iRetry) {
          switch (iCallback.onAsyncReplicationError(iException, ++retry)) {
          case RETRY:
            execute();
            break;

          case IGNORE:

          }

          return ACTION.IGNORE;
        }
      };
    } else
      onAsyncReplicationError = null;
    return this;
  }

  public OProgressListener getProgressListener() {
    return progressListener;
  }

  public OCommandRequestAbstract setProgressListener(OProgressListener progressListener) {
    this.progressListener = progressListener;
    return this;
  }

  public void reset() {
  }

  public int getLimit() {
    return limit;
  }

  public OCommandRequestAbstract setLimit(final int limit) {
    this.limit = limit;
    return this;
  }

  public String getFetchPlan() {
    return fetchPlan;
  }

  @SuppressWarnings("unchecked")
  public <RET extends OCommandRequest> RET setFetchPlan(String fetchPlan) {
    this.fetchPlan = fetchPlan;
    return (RET) this;
  }

  public boolean isUseCache() {
    return useCache;
  }

  public void setUseCache(boolean useCache) {
    this.useCache = useCache;
  }

  @Override
  public OCommandContext getContext() {
    if (context == null)
      context = new OBasicCommandContext();
    return context;
  }

  public OCommandRequestAbstract setContext(final OCommandContext iContext) {
    context = iContext;
    return this;
  }

  public long getTimeoutTime() {
    return timeoutMs;
  }

  public void setTimeout(final long timeout, final TIMEOUT_STRATEGY strategy) {
    this.timeoutMs = timeout;
    this.timeoutStrategy = strategy;
  }

  public TIMEOUT_STRATEGY getTimeoutStrategy() {
    return timeoutStrategy;
  }

  @Override
  public Set<String> nodesToExclude() {
    return Collections.unmodifiableSet(nodesToExclude);
  }

  public void addExcludedNode(String node) {
    nodesToExclude.add(node);
  }

  public void removeExcludedNode(String node) {
    nodesToExclude.remove(node);
  }


  public OAsyncReplicationOk getOnAsyncReplicationOk() {
    return onAsyncReplicationOk;
  }

  public OAsyncReplicationError getOnAsyncReplicationError() {
    return onAsyncReplicationError;
  }
}
