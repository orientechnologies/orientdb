package org.apache.tinkerpop.gremlin.orientdb.gremlintest;

import static org.junit.Assert.fail;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.orientdb.OrientEdge;
import org.apache.tinkerpop.gremlin.orientdb.OrientElement;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientProperty;
import org.apache.tinkerpop.gremlin.orientdb.OrientVertex;
import org.apache.tinkerpop.gremlin.orientdb.OrientVertexProperty;
import org.apache.tinkerpop.gremlin.structure.Graph;

public class OrientGraphProvider extends AbstractGraphProvider {
  
    static {
        File buildDir = new File("target/builddir");
        buildDir.mkdirs();
        System.setProperty("build.dir", buildDir.getAbsolutePath());
    }

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        return new HashMap<String, Object>() {{
            put(Graph.GRAPH, OrientGraph.class.getName());
            put("name", graphName);
        }};
    }

    @Override
    public Set<Class> getImplementations() {
        return new HashSet<Class>() {{
            add(OrientEdge.class);
            add(OrientElement.class);
            add(OrientGraph.class);
            add(OrientProperty.class);
            add(OrientVertex.class);
            add(OrientVertexProperty.class);
        }};
    }

    @Override
    public void clear(Graph graph, Configuration configuration) throws Exception {
        if (graph != null) graph.close();
    }

    @Override
    public Graph openTestGraph(Configuration config) {
        if(config.getString("name").equals("readGraph"))
            fail("there is some technical limitation on orientDB that makes tests enter in an infinite loop when reading and writing to orientDB");

        return super.openTestGraph(config);
    }

}
