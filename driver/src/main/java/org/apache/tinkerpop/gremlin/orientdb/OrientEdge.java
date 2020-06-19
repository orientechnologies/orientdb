package org.apache.tinkerpop.gremlin.orientdb;

import static com.google.common.base.Preconditions.checkNotNull;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.OEdgeDelegate;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

public final class OrientEdge extends OrientElement implements Edge {

  private static final List<String> INTERNAL_FIELDS = Arrays.asList("@rid", "@class", "in", "out");

  protected OVertex vOut;
  protected OVertex vIn;
  protected String label;

  public OrientEdge(
      OGraph graph, OEdge rawElement, final OVertex out, final OVertex in, final String iLabel) {
    super(graph, rawElement);
    vOut = checkNotNull(out, "out vertex on edge " + rawElement);
    vIn = checkNotNull(in, "out vertex on edge " + rawElement);
    label = checkNotNull(iLabel, "label on edge " + rawElement);
  }

  public OrientEdge(OGraph graph, OEdge rawEdge, String label) {
    this(
        graph, rawEdge, rawEdge.getVertex(ODirection.OUT), rawEdge.getVertex(ODirection.IN), label);
  }

  public OrientEdge(OGraph graph, OIdentifiable identifiable) {
    this(
        graph,
        new ODocument(identifiable.getIdentity())
            .asEdge()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format("Cannot get an Edge for identity %s", identifiable))));
  }

  public OrientEdge(OGraph graph, OEdge rawEdge) {
    this(graph, rawEdge, rawEdge.getSchemaType().get().getName());
  }

  public static OIdentifiable getConnection(
      final ODocument iEdgeRecord, final Direction iDirection) {
    return iEdgeRecord.rawField(
        iDirection == Direction.OUT
            ? OrientGraphUtils.CONNECTION_OUT
            : OrientGraphUtils.CONNECTION_IN);
  }

  protected static OEdge createRawElement(OGraph graph, String label) {
    String className = graph.createEdgeClass(label);
    OEdgeDelegate delegate = new OEdgeDelegate(new ODocument(className));
    return delegate;
  }

  @Override
  public Iterator<Vertex> vertices(Direction direction) {
    this.graph.tx().readWrite();
    switch (direction) {
      case OUT:
        return graph.vertices(vOut.getIdentity());
      case IN:
        return graph.vertices(vIn.getIdentity());
      case BOTH:
      default:
        return graph.vertices(vOut.getIdentity(), vIn.getIdentity());
    }
  }

  public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
    Iterator<? extends Property<V>> properties = super.properties(propertyKeys);
    return StreamUtils.asStream(properties)
        .filter(p -> !INTERNAL_FIELDS.contains(p.key()))
        .map(p -> (Property<V>) p)
        .iterator();
  }

  @Override
  public OElement getRawElement() {
    return rawElement
        .asEdge()
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    String.format("Cannot get an Edge for identity %s", rawElement.getIdentity())));
  }

  @Override
  public String toString() {
    return StringFactory.edgeString(this);
  }
}
