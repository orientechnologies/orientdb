package org.apache.tinkerpop.gremlin.orientdb.io.graphson;

import java.util.Map;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TinkerPopJacksonModule;

/** Created by Enrico Risa on 06/09/2017. */
public abstract class OrientGraphSON extends TinkerPopJacksonModule {

  public OrientGraphSON(String name) {
    super(name);
  }

  @Override
  public Map<Class, String> getTypeDefinitions() {
    return null;
  }

  @Override
  public String getTypeNamespace() {
    return "orient";
  }
}
