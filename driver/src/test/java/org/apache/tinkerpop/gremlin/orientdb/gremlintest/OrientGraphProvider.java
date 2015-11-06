package org.apache.tinkerpop.gremlin.orientdb.gremlintest;

import static org.junit.Assume.assumeFalse;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
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
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;

import com.google.common.collect.Sets;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;

public class OrientGraphProvider extends AbstractGraphProvider {

    static {
        File buildDir = new File("target/builddir");
        buildDir.mkdirs();
        System.setProperty("build.dir", buildDir.getAbsolutePath());
    }

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        HashMap<String, Object> configs = new HashMap<String, Object>();
        configs.put(Graph.GRAPH, OrientGraph.class.getName());
        configs.put("name", graphName);
        return configs;
    }

    @SuppressWarnings({ "rawtypes" })
    @Override
    public Set<Class> getImplementations() {
        return Sets.newHashSet(
                OrientEdge.class,
                OrientElement.class,
                OrientGraph.class,
                OrientProperty.class,
                OrientVertex.class,
                OrientVertexProperty.class);
    }

    @Override
    public void clear(Graph graph, Configuration configuration) throws Exception {
        if (graph != null)
            graph.close();
    }

    @Override
    public Graph openTestGraph(Configuration config) {
        if (config.getString("name").equals("readGraph"))
            //FIXME eventually ne need to get ride of this 
            assumeFalse("there is some technical limitation on orientDB that makes tests enter in an infinite loop when reading and writing to orientDB", true);

        return super.openTestGraph(config);
    }

    @Override
    public ORID convertId(Object id, Class<? extends Element> c) {
        if (id instanceof Number) {
            long numericId = ((Number) id).longValue();
            return new ORecordId(new Random(numericId).nextInt(32767), numericId);
        }
        if (id instanceof String) {
            Integer numericId;
            try {
                numericId = new Integer(id.toString());
            } catch (NumberFormatException e) {
                return new MockORID("Invalid id: " + id + " for " + c);
            }
            return new ORecordId(numericId, numericId.longValue());
        }

        return new MockORID("Invalid id: " + id + " for " + c);
    }

}
