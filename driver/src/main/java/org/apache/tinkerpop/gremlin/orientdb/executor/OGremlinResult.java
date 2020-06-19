package org.apache.tinkerpop.gremlin.orientdb.executor;

import com.orientechnologies.orient.core.sql.executor.OResult;
import java.util.Optional;
import java.util.Spliterator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.tinkerpop.gremlin.orientdb.OrientEdge;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientVertex;

/** Created by Enrico Risa on 05/06/2017. */
public class OGremlinResult {

  private OrientGraph graph;
  OResult inner;

  public OGremlinResult(OrientGraph graph, OResult inner) {
    this.graph = graph;
    this.inner = inner;
  }

  public <T> T getProperty(String name) {
    Object value = inner.getProperty(name);
    if (value instanceof Iterable) {
      Spliterator spliterator = ((Iterable) value).spliterator();
      value =
          StreamSupport.stream(spliterator, false)
              .map(
                  (e) -> {
                    if (e instanceof OResult) {
                      return new OGremlinResult(this.graph, (OResult) e);
                    } else {
                      return e;
                    }
                  })
              .collect(Collectors.toList());
    }
    return (T) value;
  }

  public Optional<OrientVertex> getVertex() {
    return inner.getVertex().map((v) -> graph.elementFactory().wrapVertex(v));
  }

  public Optional<OrientEdge> getEdge() {
    return inner.getEdge().map((v) -> graph.elementFactory().wrapEdge(v));
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
