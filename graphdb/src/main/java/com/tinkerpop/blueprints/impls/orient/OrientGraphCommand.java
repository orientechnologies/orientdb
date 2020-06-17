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

package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext.TIMEOUT_STRATEGY;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import java.util.Iterator;
import java.util.Map;

/**
 * Command wraps the command request and return wrapped Graph Element such as Vertex and Edge
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com) (http://orientdb.com)
 */
public class OrientGraphCommand implements OCommandRequest {
  private OrientBaseGraph graph;
  private OCommandRequest underlying;

  public OrientGraphCommand(final OrientBaseGraph iGraph, final OCommandRequest iCommand) {
    graph = iGraph;
    underlying = iCommand;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET> RET execute(final Object... iArgs) {
    Object result = underlying.execute(iArgs);
    if (!(result instanceof Map) && OMultiValue.isMultiValue(result)) {
      result = new OrientDynaElementIterable(graph, ((Iterable<?>) result).iterator());
    } else if (result instanceof Iterator<?>) {
      result = new OrientDynaElementIterable(graph, (Iterator<?>) result);
    } else if (result instanceof OIdentifiable) {
      result = graph.getElement(result);
    }

    return (RET) result;
  }

  @Override
  public int getLimit() {
    return underlying.getLimit();
  }

  @Override
  public OCommandRequest setLimit(final int iLimit) {
    underlying.setLimit(iLimit);
    return this;
  }

  @Override
  public long getTimeoutTime() {
    return underlying.getTimeoutTime();
  }

  @Override
  public TIMEOUT_STRATEGY getTimeoutStrategy() {
    return underlying.getTimeoutStrategy();
  }

  @Override
  public void setTimeout(final long timeout, final TIMEOUT_STRATEGY strategy) {
    underlying.setTimeout(timeout, strategy);
  }

  @Override
  public boolean isIdempotent() {
    return underlying.isIdempotent();
  }

  @Override
  public String getFetchPlan() {
    return underlying.getFetchPlan();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends OCommandRequest> RET setFetchPlan(final String iFetchPlan) {
    underlying.setFetchPlan(iFetchPlan);
    return (RET) this;
  }

  @Override
  public void setUseCache(final boolean iUseCache) {
    underlying.setUseCache(iUseCache);
  }

  @Override
  public OCommandContext getContext() {
    return underlying.getContext();
  }

  @Override
  public OCommandRequest setContext(final OCommandContext iContext) {
    underlying.setContext(iContext);
    return this;
  }
}
