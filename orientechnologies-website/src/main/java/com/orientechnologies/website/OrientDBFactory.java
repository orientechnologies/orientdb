package com.orientechnologies.website;

import org.springframework.stereotype.Component;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;

@Component
public class OrientDBFactory {

  private OrientGraphFactory factory;

  ThreadLocal<OrientGraph>   graphThreadLocal = new ThreadLocal<OrientGraph>();

  public OrientDBFactory() {
    factory = new OrientGraphFactory("plocal:databases/odbsite", "admin", "admin");
  }

  public OrientGraph getDb() {

    OrientGraph graph = graphThreadLocal.get();
    if (graph == null) {
      graph = factory.getTx();
      graphThreadLocal.set(graph);
    }
    return graph;
  }

  public void unsetDb() {
    graphThreadLocal.set(null);
  }
}
