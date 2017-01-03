/*
 *
 *  *  Copyright 2015 OrientDB LTD (info(at)orientdb.com)
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
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.orient.graph.sql.functions;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionFiltered;
import com.orientechnologies.orient.graph.sql.OGraphCommandExecutorSQLFactory;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public abstract class OSQLFunctionMoveFiltered extends OSQLFunctionMove implements OSQLFunctionFiltered {

    protected static int supernodeThreshold = 1000; // move to some configuration

  public OSQLFunctionMoveFiltered() {
      super(NAME, 1, 2);
    }

  public OSQLFunctionMoveFiltered(final String iName, final int iMin, final int iMax) {
      super(iName, iMin, iMax);
    }

    @Override
    public Object execute(final Object iThis, final OIdentifiable iCurrentRecord, final Object iCurrentResult,
    final Object[] iParameters, final Iterable<OIdentifiable> iPossibleResults, final OCommandContext iContext) {
      return OGraphCommandExecutorSQLFactory.runWithAnyGraph(new OGraphCommandExecutorSQLFactory.GraphCallBack<Object>() {
        @Override
        public Object call(final OrientBaseGraph graph) {
          final String[] labels;
          if (iParameters != null && iParameters.length > 0 && iParameters[0] != null)
            labels = OMultiValue.array(iParameters, String.class, new OCallable<Object, Object>() {

              @Override
              public Object call(final Object iArgument) {
                return OIOUtils.getStringContent(iArgument);
              }
            });
          else
            labels = null;

          return OSQLEngine.foreachRecord(new OCallable<Object, OIdentifiable>() {
            @Override
            public Object call(final OIdentifiable iArgument) {
              return move(graph, iArgument, labels, iPossibleResults);
            }
          }, iThis, iContext);
        }
      });
    }

  protected abstract Object move(OrientBaseGraph graph, OIdentifiable iArgument, String[] labels,
      Iterable<OIdentifiable> iPossibleResults);

}