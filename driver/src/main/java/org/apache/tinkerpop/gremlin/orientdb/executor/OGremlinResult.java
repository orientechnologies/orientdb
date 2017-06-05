package org.apache.tinkerpop.gremlin.orientdb.executor;

import com.orientechnologies.orient.core.sql.executor.OResult;
import org.apache.tinkerpop.gremlin.orientdb.OrientEdge;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientVertex;

import java.util.Optional;

/**
 * Created by Enrico Risa on 05/06/2017.
 */
public class OGremlinResult {

    private OrientGraph graph;
    OResult inner;

    public OGremlinResult(OrientGraph graph, OResult inner) {
        this.graph = graph;
        this.inner = inner;
    }

    public <T> T getProperty(String name) {
        return inner.getProperty(name);
    }

    public Optional<OrientVertex> getVertex() {
        return inner.getVertex().map((v) -> new OrientVertex(graph, v));
    }

    public Optional<OrientEdge> getEdge() {
        return inner.getEdge().map((v) -> new OrientEdge(graph, v));
    }

    public boolean isElement() {
        return inner.isElement();
    }

    public boolean isVertex() {
        return inner.isVertex();
    }

    public boolean isEdge() {
        return inner.isEdge();
    }

    public boolean isBlob() {
        return inner.isBlob();
    }

    public OResult getRawResult() {
        return inner;
    }
}
