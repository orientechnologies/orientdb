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
package com.orientechnologies.orient.graph.stresstest;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.stresstest.ODatabaseIdentifier;
import com.orientechnologies.orient.stresstest.ODatabaseUtils;
import com.orientechnologies.orient.stresstest.workload.OBaseWorkload;
import com.orientechnologies.orient.stresstest.workload.OCheckWorkload;
import com.tinkerpop.blueprints.impls.orient.OGraphRepair;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

/**
 * CRUD implementation of the workload.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public abstract class OBaseGraphWorkload extends OBaseWorkload implements OCheckWorkload {
  public class OWorkLoadContext extends OBaseWorkload.OBaseWorkLoadContext {
    protected OrientBaseGraph graph;
    protected OrientVertex lastVertexToConnect;
    protected int lastVertexEdges;

    @Override
    public void init(ODatabaseIdentifier dbIdentifier, int operationsPerTransaction) {
      graph = operationsPerTransaction > 0 ? getGraph(dbIdentifier) : getGraphNoTx(dbIdentifier);
    }

    @Override
    public void close() {
      if (graph != null) graph.shutdown();
    }
  }

  @Override
  protected OBaseWorkLoadContext getContext() {
    return new OWorkLoadContext();
  }

  protected OrientGraphNoTx getGraphNoTx(final ODatabaseIdentifier databaseIdentifier) {
    final ODatabase database = ODatabaseUtils.openDatabase(databaseIdentifier, connectionStrategy);
    if (database == null)
      throw new IllegalArgumentException(
          "Error on opening database " + databaseIdentifier.getName());

    return (OrientGraphNoTx)
        OrientGraphFactory.getNoTxGraphImplFactory().getGraph((ODatabaseDocumentTx) database);
  }

  protected OrientGraph getGraph(final ODatabaseIdentifier databaseIdentifier) {
    final ODatabase database = ODatabaseUtils.openDatabase(databaseIdentifier, connectionStrategy);
    if (database == null)
      throw new IllegalArgumentException(
          "Error on opening database " + databaseIdentifier.getName());

    final OrientGraph g =
        (OrientGraph)
            OrientGraphFactory.getTxGraphImplFactory().getGraph((ODatabaseDocumentTx) database);
    g.setAutoStartTx(false);
    return g;
  }

  @Override
  public void check(final ODatabaseIdentifier databaseIdentifier) {
    final OGraphRepair repair = new OGraphRepair();
    repair.repair(
        getGraphNoTx(databaseIdentifier),
        new OCommandOutputListener() {
          @Override
          public void onMessage(String iText) {
            System.out.print("   - " + iText);
          }
        },
        null);
  }

  @Override
  protected void beginTransaction(final OBaseWorkLoadContext context) {
    ((OWorkLoadContext) context).graph.begin();
  }

  @Override
  protected void commitTransaction(final OBaseWorkLoadContext context) {
    ((OWorkLoadContext) context).graph.commit();
  }
}
