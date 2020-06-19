package org.apache.tinkerpop.gremlin.orientdb.jsr223;

import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer;
import org.apache.tinkerpop.gremlin.orientdb.*;

/** Created by Enrico Risa on 06/02/17. */
public class OrientDBGremlinPlugin extends AbstractGremlinPlugin {

  private static final String NAME = "tinkerpop.orientdb";

  private static final ImportCustomizer imports;

  static {
    try {
      imports =
          DefaultImportCustomizer.build()
              .addClassImports(
                  OrientEdge.class,
                  OrientElement.class,
                  OrientGraph.class,
                  OrientStandardGraph.class,
                  ODBFeatures.OrientVariableFeatures.class,
                  OrientProperty.class,
                  OrientVertex.class,
                  OrientVertexProperty.class)
              .create();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public OrientDBGremlinPlugin() {
    super(NAME, imports);
  }

  private static final OrientDBGremlinPlugin instance = new OrientDBGremlinPlugin();

  public static OrientDBGremlinPlugin instance() {
    return instance;
  }

  @Override
  public String getName() {
    return NAME;
  }
}
