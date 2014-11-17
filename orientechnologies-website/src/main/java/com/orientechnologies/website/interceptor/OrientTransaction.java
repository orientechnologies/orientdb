package com.orientechnologies.website.interceptor;

import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;

/**
 * Created by Enrico Risa on 17/11/14.
 */
public class OrientTransaction {

  /** The orient tx object. */
  private OTransaction         tx;

  /** The database. */
  private ODatabaseInternal<?> database;

  private OrientBaseGraph      graph;

  public OTransaction getTx() {
    return tx;
  }

  public void setGraph(OrientBaseGraph graph) {
    this.graph = graph;
  }

  public OrientBaseGraph getGraph() {
    return graph;
  }

  public void setTx(OTransaction tx) {
    this.tx = tx;
  }

  public ODatabaseInternal<?> getDatabase() {
    return getGraph().getRawGraph();
  }

}
