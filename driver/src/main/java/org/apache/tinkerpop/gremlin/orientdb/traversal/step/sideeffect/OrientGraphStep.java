package org.apache.tinkerpop.gremlin.orientdb.traversal.step.sideeffect;

import com.google.common.annotations.VisibleForTesting;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.tinkerpop.gremlin.orientdb.*;
import org.apache.tinkerpop.gremlin.orientdb.executor.OGremlinResult;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.DefaultCloseableIterator;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.TriFunction;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

public class OrientGraphStep<S, E extends Element> extends GraphStep<S, E>
    implements HasContainerHolder {

  private static final long serialVersionUID = 8141248670294067626L;

  private final List<HasContainer> hasContainers = new ArrayList<>();

  public OrientGraphStep(final GraphStep<S, E> originalGraphStep) {
    super(
        originalGraphStep.getTraversal(),
        originalGraphStep.getReturnClass(),
        originalGraphStep.isStartStep(),
        originalGraphStep.getIds());
    originalGraphStep.getLabels().forEach(this::addLabel);
    this.setIteratorSupplier(() -> (Iterator<E>) (isVertexStep() ? this.vertices() : this.edges()));
  }

  public boolean isVertexStep() {
    return Vertex.class.isAssignableFrom(this.returnClass);
  }

  private Iterator<? extends Vertex> vertices() {
    return elements(
        OGraph::vertices, OGraph::getIndexedVertices, OGraph::vertices, v -> v.getVertex().get());
  }

  private Iterator<? extends Edge> edges() {
    return elements(
        OGraph::edges, OGraph::getIndexedEdges, OGraph::edges, (e) -> e.getEdge().get());
  }

  /**
   * Gets an iterator over those vertices/edges that have the specific IDs wanted, or those that
   * have indexed properties with the wanted values, or failing that by just getting all of the
   * vertices or edges.
   *
   * @param getElementsByIds Function that will return an iterator over all the vertices/edges in
   *     the graph that have the specific IDs
   * @param getElementsByIndex Function that returns a stream of all the vertices/edges in the graph
   *     that have an indexed property with a specific value
   * @param getAllElements Function that returns an iterator of all the vertices or all the edges
   *     (i.e. full scan)
   * @return An iterator for all the vertices/edges for this step
   */
  private <ElementType extends Element> Iterator<? extends ElementType> elements(
      BiFunction<OGraph, Object[], Iterator<ElementType>> getElementsByIds,
      TriFunction<OGraph, OIndex, Iterator<Object>, Stream<? extends ElementType>>
          getElementsByIndex,
      Function<OGraph, Iterator<ElementType>> getAllElements,
      Function<OGremlinResult, ElementType> getElement) {
    final OGraph graph = getGraph();

    if (this.ids != null && this.ids.length > 0) {
      /** Got some element IDs, so just get the elements using those */
      return this.iteratorList(getElementsByIds.apply(graph, this.ids));
    } else {
      Optional<? extends Iterator<? extends ElementType>> streamIterator =
          buildQuery()
              .map(
                  (query) ->
                      query.execute(getGraph()).stream()
                          .map(getElement::apply)
                          .filter(element -> HasContainer.testAll(element, this.hasContainers)))
              .map(
                  stream ->
                      new DefaultCloseableIterator<ElementType>(stream.iterator()) {
                        @Override
                        public void close() {
                          stream.close();
                        }
                      });
      if (streamIterator.isPresent()) {
        return streamIterator.get();
      } else {
        return this.iteratorList(getAllElements.apply(graph));
      }
    }
  }

  private boolean isLabelKey(String key) {
    try {
      return T.fromString(key) == T.label;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  // if one of the HasContainers is a label matching predicate, then return those labels
  private Set<String> findClassLabelsInHasContainers() {
    Set<String> classLabels = new HashSet<>();

    HasContainer container =
        this.hasContainers.stream()
            .filter(hasContainer -> isLabelKey(hasContainer.getKey()))
            .findFirst()
            .orElseGet(
                () -> {
                  String defaultClass = Vertex.class.isAssignableFrom(getReturnClass()) ? "V" : "E";
                  HasContainer defaultContainer =
                      new HasContainer(T.label.name(), P.eq(defaultClass));
                  return defaultContainer;
                });

    Object value = container.getValue();

    // The ugly part. Is there anyway to know the return type of a predicate value ?
    if (value instanceof List) {
      ((List) value).forEach(label -> classLabels.add((String) label));
    } else {
      classLabels.add((String) value);
    }

    return classLabels;
  }

  private OGraph getGraph() {
    return (OGraph) this.getTraversal().getGraph().get();
  }

  public Optional<OrientGraphBaseQuery> buildQuery() {

    OrientGraphQueryBuilder builder = new OrientGraphQueryBuilder(isVertexStep());
    this.hasContainers.forEach(builder::addCondition);
    return builder.build(getGraph());
  }

  @VisibleForTesting
  public Set<OrientIndexQuery> findIndex() {
    final Set<OrientIndexQuery> indexedQueries = new HashSet<>();
    final OGraph graph = getGraph();
    final OIndexManager indexManager = graph.getRawDatabase().getMetadata().getIndexManager();

    // find indexed keys only for the element subclasses (if present)
    final Set<String> classLabels = findClassLabelsInHasContainers();

    if (!classLabels.isEmpty()) {
      final Set<String> indexedKeys = new HashSet<>();
      classLabels.forEach(
          label -> indexedKeys.addAll(graph.getIndexedKeys(this.returnClass, label)));
      this.hasContainers.stream()
          .filter(
              c ->
                  indexedKeys.contains(c.getKey())
                      && (c.getPredicate().getBiPredicate() == Compare.eq
                          || c.getPredicate().getBiPredicate() == Contains.within))
          .findAny()
          .ifPresent(
              requestedKeyValue -> {
                String key = requestedKeyValue.getKey();

                classLabels.forEach(
                    classLabel -> {
                      Iterator<Object> values = getValueIterator(requestedKeyValue);
                      String className =
                          graph.labelToClassName(
                              classLabel,
                              isVertexStep() ? OClass.VERTEX_CLASS_NAME : OClass.EDGE_CLASS_NAME);
                      Set<OIndex> classIndexes = indexManager.getClassIndexes(className);
                      Iterator<OIndex> keyIndexes =
                          classIndexes.stream()
                              .filter(idx -> idx.getDefinition().getFields().contains(key))
                              .iterator();

                      if (keyIndexes.hasNext()) {
                        // TODO: select best index if there are multiple options
                        indexedQueries.add(new OrientIndexQuery(keyIndexes.next(), values));
                      } else {
                        OLogManager.instance()
                            .warn(
                                this,
                                "no index found for class=["
                                    + className
                                    + "] and key=["
                                    + key
                                    + "]");
                      }
                    });
              });
    }

    return indexedQueries;
  }

  /**
   * gets the requested values from the Has step. If it's a single value, wrap it in an array,
   * otherwise return the array
   */
  private Iterator<Object> getValueIterator(HasContainer c) {
    return c.getPredicate().getBiPredicate() == Contains.within
        ? ((Iterable<Object>) c.getValue()).iterator()
        : IteratorUtils.of(c.getValue());
  }

  @Override
  public String toString() {
    if (this.hasContainers.isEmpty()) return super.toString();
    else
      return 0 == this.ids.length
          ? StringFactory.stepString(
              this, this.returnClass.getSimpleName().toLowerCase(), this.hasContainers)
          : StringFactory.stepString(
              this,
              this.returnClass.getSimpleName().toLowerCase(),
              Arrays.toString(this.ids),
              this.hasContainers);
  }

  private <X extends Element> Iterator<X> iteratorList(final Iterator<X> iterator) {
    final List<X> list = new ArrayList<>();
    while (iterator.hasNext()) {
      final X e = iterator.next();
      if (HasContainer.testAll(e, this.hasContainers)) list.add(e);
    }
    return list.iterator();
  }

  @Override
  public List<HasContainer> getHasContainers() {
    return Collections.unmodifiableList(this.hasContainers);
  }

  @Override
  public void addHasContainer(final HasContainer hasContainer) {
    this.hasContainers.add(hasContainer);
  }
}
