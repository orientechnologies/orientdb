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
