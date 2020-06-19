package org.apache.tinkerpop.gremlin.orientdb;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static org.apache.tinkerpop.gremlin.orientdb.StreamUtils.asStream;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

public final class OrientVertex extends OrientElement implements Vertex {

  private static final List<String> INTERNAL_FIELDS = Arrays.asList("@rid", "@class");

  public OrientVertex(final OGraph graph, final OVertex rawElement) {
    super(graph, rawElement);
  }

  public OrientVertex(OGraph graph, OIdentifiable identifiable) {
    this(
        graph,
        new ODocument(identifiable.getIdentity())
            .asVertex()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format("Cannot get a Vertex for identity %s", identifiable))));
  }

  public OrientVertex(OGraph graph, String label) {
    this(graph, createRawElement(graph, label));
  }

  protected static OVertex createRawElement(OGraph graph, String label) {
    graph.createVertexClass(label);
    return graph.getRawDatabase().newVertex(label);
  }

  public Iterator<Vertex> vertices(final Direction direction, final String... labels) {
    this.graph.tx().readWrite();
    Stream<Vertex> vertexStream =
        asStream(
                getRawElement()
                    .getVertices(OrientGraphUtils.mapDirection(direction), labels)
                    .iterator())
            .map(v -> graph.elementFactory().wrapVertex(v));
    return vertexStream.iterator();
  }

  @Override
  public <V> VertexProperty<V> property(String key) {

    ODocument doc = getRawElement().getRecord();
    if (doc.containsField(key)) {
      return new OrientVertexProperty<>(key, getRawElement().getProperty(key), this);
    }
    return VertexProperty.empty();
  }

  public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
    Iterator<? extends Property<V>> properties = super.properties(propertyKeys);
    return StreamUtils.asStream(properties)
        .filter(p -> !INTERNAL_FIELDS.contains(p.key()))
        .filter(p -> !p.key().startsWith("out_"))
        .filter(p -> !p.key().startsWith("in_"))
        .filter(p -> !p.key().startsWith("_meta_"))
        .map(
            p ->
                (VertexProperty<V>)
                    new OrientVertexProperty<>(p.key(), p.value(), (OrientVertex) p.element()))
        .iterator();
  }

  @Override
  public OVertex getRawElement() {
    return rawElement
        .asVertex()
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    String.format("Cannot get a Vertex for element %s", rawElement)));
  }

  @Override
  public <V> VertexProperty<V> property(final String key, final V value) {
    return new OrientVertexProperty<>(super.property(key, value), this);
  }

  @Override
  public <V> VertexProperty<V> property(
      final String key, final V value, final Object... keyValues) {
    VertexProperty<V> vertexProperty = this.property(key, value);

    if (ElementHelper.getIdValue(keyValues).isPresent())
      throw VertexProperty.Exceptions.userSuppliedIdsNotSupported();

    ElementHelper.legalPropertyKeyValueArray(keyValues);
    ElementHelper.attachProperties(vertexProperty, keyValues);
    return vertexProperty;
  }

  @Override
  public <V> VertexProperty<V> property(
      final VertexProperty.Cardinality cardinality,
      final String key,
      final V value,
      final Object... keyValues) {
    return this.property(key, value, keyValues);
  }

  @Override
  public String toString() {
    return StringFactory.vertexString(this);
  }

  @Override
  public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
    if (inVertex == null) throw new IllegalArgumentException("destination vertex is null");
    checkArgument(!isNullOrEmpty(label), "label is invalid");

    ElementHelper.legalPropertyKeyValueArray(keyValues);
    if (ElementHelper.getIdValue(keyValues).isPresent())
      throw Vertex.Exceptions.userSuppliedIdsNotSupported();
    if (Graph.Hidden.isHidden(label)) throw Element.Exceptions.labelCanNotBeAHiddenKey(label);

    graph.createEdgeClass(label);

    this.graph.tx().readWrite();

    OEdge oEdge = getRawElement().addEdge(((OrientVertex) inVertex).getRawElement(), label);
    final OrientEdge edge = graph.elementFactory().wrapEdge(oEdge);
    edge.property(keyValues);

    edge.save();
    return edge;
  }

  public Iterator<Edge> edges(final Direction direction, String... edgeLabels) {
    this.graph.tx().readWrite();
    // It should not collect but instead iterating through the relations.
    // But necessary in order to avoid loop in
    // EdgeTest#shouldNotHaveAConcurrentModificationExceptionWhenIteratingAndRemovingAddingEdges
    Stream<Edge> edgeStream =
        asStream(
                getRawElement()
                    .getEdges(OrientGraphUtils.mapDirection(direction), edgeLabels)
                    .iterator())
            .map(e -> graph.elementFactory().wrapEdge(e));

    return edgeStream.collect(Collectors.toList()).iterator();
  }
}
