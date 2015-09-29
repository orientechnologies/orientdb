/*
 *
 *  *  Copyright 2015 Orient Technologies LTD (info(at)orientdb.com)
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
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionFiltered;
import com.orientechnologies.orient.graph.sql.OGraphCommandExecutorSQLFactory;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;

/**
 * @author Luigi Dell'Aquila
 */
public abstract class OSQLFunctionMoveFiltered extends OSQLFunctionMove implements OSQLFunctionFiltered{

  public OSQLFunctionMoveFiltered() {
    super(NAME, 1, 2);
  }

  public OSQLFunctionMoveFiltered(final String iName, final int iMin, final int iMax) {
    super(iName, iMin, iMax);
  }


  @Override public Object execute(Object iThis, OIdentifiable iCurrentRecord, Object iCurrentResult, Object[] iParameters,
      final Iterable<OIdentifiable> iPossibleResults, OCommandContext iContext) {
    final OModifiableBoolean shutdownFlag = new OModifiableBoolean();
    ODatabaseDocumentInternal curDb = ODatabaseRecordThreadLocal.INSTANCE.get();
    final OrientBaseGraph graph = OGraphCommandExecutorSQLFactory.getAnyGraph(shutdownFlag);
    try {
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
    } finally {
      if (shutdownFlag.getValue())
        graph.shutdown(false);
      ODatabaseRecordThreadLocal.INSTANCE.set(curDb);
    }
  }

  protected abstract Object move(OrientBaseGraph graph, OIdentifiable iArgument, String[] labels,
      Iterable<OIdentifiable> iPossibleResults);

}