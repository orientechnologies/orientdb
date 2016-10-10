/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (info(-at-)orientdb.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.orient.etl;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;

/**
 * Created by frank on 23/09/2016.
 */
public class OETLDatabaseProvider {

  private final ODatabaseDocument db;
  private final OrientBaseGraph   graph;

  public OETLDatabaseProvider(ODatabaseDocument db, OrientBaseGraph graph) {
    this.db = db;
    this.graph = graph;
  }

  public ODatabaseDocument getDocumentDatabase() {
    db.activateOnCurrentThread();
    return db;
  }

  public OrientBaseGraph getGraphDatabase() {
    graph.makeActive();
    return graph;
  }

  public void commit() {
    db.activateOnCurrentThread().commit();
    graph.commit();
  }
}
