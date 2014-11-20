package com.orientechnologies.website;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class OrientDBFactory {

  private OrientGraphFactory         factory;

  @Autowired
  private OrientDBConnectionSettings settings;

  ThreadLocal<OrientGraph>           graphThreadLocal = new ThreadLocal<OrientGraph>();

  public OrientDBFactory() {

  }

  @PostConstruct
  public void initFactory() {
    factory = new OrientGraphFactory(settings.getUrl(), settings.getUsr(), settings.getPwd());
  }

  public OrientGraph getGraph() {

    OrientGraph graph = graphThreadLocal.get();
    if (graph == null) {
      graph = factory.getTx();
      graphThreadLocal.set(graph);
    }
    return graph;
  }

  public OrientGraphNoTx getGraphtNoTx() {
    return factory.getNoTx();
  }

  public void unsetDb() {
    graphThreadLocal.set(null);
  }

}
