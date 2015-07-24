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
package com.orientechnologies.orient.graph.script;

import com.orientechnologies.orient.core.command.script.OScriptOrientWrapper;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;

/**
 * Blueprints Graph wrapper class to use from scripts.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OScriptGraphOrientWrapper extends OScriptOrientWrapper {

  public OScriptGraphOrientWrapper() {
    super(ODatabaseRecordThreadLocal.INSTANCE.get().getDatabaseOwner());
  }

  public OScriptGraphWrapper getGraphNoTx() {
    final ODatabaseDocumentTx threadDatabase = (ODatabaseDocumentTx) ODatabaseRecordThreadLocal.INSTANCE.get().getDatabaseOwner();
    return new OScriptGraphWrapper(new OrientGraphNoTx(threadDatabase));
  }

  public OScriptGraphWrapper getGraph() {
    final ODatabaseDocumentTx threadDatabase = (ODatabaseDocumentTx) ODatabaseRecordThreadLocal.INSTANCE.get().getDatabaseOwner();
    return new OScriptGraphWrapper(new OrientGraph(threadDatabase));
  }
}
