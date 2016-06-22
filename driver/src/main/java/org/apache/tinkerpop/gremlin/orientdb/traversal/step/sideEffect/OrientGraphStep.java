package org.apache.tinkerpop.gremlin.orientdb.traversal.step.sideEffect;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.tinkerpop.gremlin.orientdb.OrientEdge;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientIndexQuery;
import org.apache.tinkerpop.gremlin.orientdb.OrientVertex;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.TriFunction;
import org.javatuples.Pair;

import com.google.common.annotations.VisibleForTesting;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManagerProxy;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;

public class OrientGraphStep<S, E extends Element> extends GraphStep<S, E>implements HasContainerHolder {

    private final List<HasContainer> hasContainers = new ArrayList<>();

    public OrientGraphStep(final GraphStep<S, E> originalGraphStep) {
        super(originalGraphStep.getTraversal(), originalGraphStep.getReturnClass(), originalGraphStep.isStartStep(), originalGraphStep.getIds());
        originalGraphStep.getLabels().forEach(this::addLabel);
        this.setIteratorSupplier(() -> (Iterator<E>) (isVertexStep() ? this.vertices() : this.edges()));
    }

    private boolean isVertexStep() {
        return Vertex.class.isAssignableFrom(this.returnClass);
    }

    private Iterator<? extends Vertex> vertices() {
        return elements(OrientGraph::vertices, OrientGraph::getIndexedVertices, OrientGraph::vertices);
    }

    private Iterator<? extends Edge> edges() {
        return elements(OrientGraph::edges, OrientGraph::getIndexedEdges, OrientGraph::edges);
    }

    /**
     * Gets an iterator over those vertices/edges that have the specific IDs
     * wanted, or those that have indexed properties with the wanted values, or
     * failing that by just getting all of the vertices or edges.
     * 
     * @param getElementsByIds
     *            Function that will return an iterator over all the
     *            vertices/edges in the graph that have the specific IDs
     * @param getElementsByIndex
     *            Function that returns a stream of all the vertices/edges in
     *            the graph that have an indexed property with a specific value
     * @param getAllElements
     *            Function that returns an iterator of all the vertices or all
     *            the edges (i.e. full scan)
     * @return An iterator for all the vertices/edges for this step
     */
    private <ElementType extends Element> Iterator<? extends ElementType> elements(BiFunction<OrientGraph, Object[], Iterator<ElementType>> getElementsByIds,
            TriFunction<OrientGraph, OIndex<Object>, Optional<Object>, Stream<? extends ElementType>> getElementsByIndex,
            Function<OrientGraph, Iterator<ElementType>> getAllElements) {
        final OrientGraph graph = getGraph();

        if (this.ids != null && this.ids.length > 0) {
            /** Got some element IDs, so just get the elements using those */
            return this.iteratorList(getElementsByIds.apply(graph, this.ids));
        } else {
            /** Have no element IDs. See if there's an indexed property to use */
            Optional<OrientIndexQuery> indexQueryOption = findIndex();
            if (indexQueryOption.isPresent()) {
                OrientIndexQuery indexQuery = indexQueryOption.get();
                OLogManager.instance().debug(this, "using " + indexQuery);

                Stream<? extends ElementType> indexedElements = getElementsByIndex.apply(graph, indexQuery.index, indexQuery.value);
                return indexedElements
                        .filter(element -> HasContainer.testAll(element, this.hasContainers))
                        .collect(Collectors.<ElementType> toList())
                        .iterator();

            } else {
                OLogManager.instance().warn(this, "scanning through all elements without using an index for Traversal " + getTraversal());
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

    // if one of the HasContainers is a label matching predicate, then return
    // that label
    private Optional<String> findElementLabelInHasContainers() {
        return this.hasContainers.stream()
                .filter(hasContainer -> isLabelKey(hasContainer.getKey()))
                .findFirst()
                .map(hasContainer -> hasContainer.getValue().toString());
    }

    private OrientGraph getGraph() {
        return (OrientGraph) this.getTraversal().getGraph().get();
    }

    @VisibleForTesting
    public Optional<OrientIndexQuery> findIndex() {
        final Optional<String> elementLabel = findElementLabelInHasContainers();
        final OrientGraph graph = getGraph();
        final OIndexManagerProxy indexManager = graph.database().getMetadata().getIndexManager();

        // find indexed keys only for the element subclass (if present)
        final Set<String> indexedKeys = elementLabel.isPresent() ? graph.getIndexedKeys(this.returnClass, elementLabel.get()) : graph.getIndexedKeys(this.returnClass);

        Optional<Pair<String, Object>> indexedKeyAndValue = this.hasContainers.stream()
                .filter(c -> indexedKeys.contains(c.getKey()) && c.getPredicate().getBiPredicate() == Compare.eq)
                .findAny()
                .map(c -> Optional.of(new Pair<>(c.getKey(), c.getValue())))
                .orElseGet(Optional::empty);

        if (elementLabel.isPresent() && indexedKeyAndValue.isPresent()) {
            String key = indexedKeyAndValue.get().getValue0();
            Object value = indexedKeyAndValue.get().getValue1();

            String className = graph.labelToClassName(elementLabel.get(), isVertexStep() ? OImmutableClass.VERTEX_CLASS_NAME : OImmutableClass.EDGE_CLASS_NAME);
            Set<OIndex<?>> classIndexes = indexManager.getClassIndexes(className);
            Iterator<OIndex<?>> keyIndexes = classIndexes.stream().filter(idx -> idx.getDefinition().getFields().contains(key)).iterator();

            if (keyIndexes.hasNext()) {
                // TODO: implement algorithm to select best index if there are
                // multiple
                return Optional.of(new OrientIndexQuery(keyIndexes.next(), Optional.of(value)));
            } else {
                OLogManager.instance().warn(this, "no index found for class=[" + className + "] and key=[" + key + "]");
            }
        }

        return Optional.empty();
    }

    @Override
    public String toString() {
        if (this.hasContainers.isEmpty())
            return super.toString();
        else
            return 0 == this.ids.length ? StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), this.hasContainers)
                    : StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), Arrays.toString(this.ids), this.hasContainers);
    }

    private <E extends Element> Iterator<E> iteratorList(final Iterator<E> iterator) {
        final List<E> list = new ArrayList<>();
        while (iterator.hasNext()) {
            final E e = iterator.next();
            if (HasContainer.testAll(e, this.hasContainers))
                list.add(e);
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
