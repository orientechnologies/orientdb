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
package com.orientechnologies.orient.graph.script;

import com.orientechnologies.orient.core.command.script.OScriptOrientWrapper;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;

/**
 * Blueprints Graph wrapper class to use from scripts.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OScriptGraphOrientWrapper extends OScriptOrientWrapper {

  public OScriptGraphOrientWrapper() {
    super(ODatabaseRecordThreadLocal.instance().get().getDatabaseOwner());
  }

  public OScriptGraphWrapper getGraphNoTx() {
    final ODatabaseDocumentInternal threadDatabase =
        (ODatabaseDocumentInternal) ODatabaseRecordThreadLocal.instance().get().getDatabaseOwner();
    return new OScriptGraphWrapper(
        OrientGraphFactory.getNoTxGraphImplFactory().getGraph(threadDatabase));
  }

  public OScriptGraphWrapper getGraph() {
    final ODatabaseDocumentInternal threadDatabase =
        (ODatabaseDocumentInternal) ODatabaseRecordThreadLocal.instance().get().getDatabaseOwner();
    return new OScriptGraphWrapper(
        OrientGraphFactory.getTxGraphImplFactory().getGraph(threadDatabase));
  }
}
