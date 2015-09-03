package org.apache.tinkerpop.gremlin.orientdb;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class OrientGraphProvider extends AbstractGraphProvider {
    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        return new HashMap<String, Object>() {{
            put(Graph.GRAPH, OrientGraph.class.getName());
        }};
    }

    @Override
    public Set<Class> getImplementations() {
        return new HashSet<Class>() {{
//            add(OrientEdge.class);
            //TODO: other classes
        }};
    }

    @Override
    public void clear(Graph graph, Configuration configuration) throws Exception {
        if (graph != null) graph.close();
    }
}
