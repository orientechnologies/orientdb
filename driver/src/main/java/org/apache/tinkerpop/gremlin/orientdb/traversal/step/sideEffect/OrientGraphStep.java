package org.apache.tinkerpop.gremlin.orientdb.traversal.step.sideEffect;

import com.google.common.annotations.VisibleForTesting;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManagerProxy;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientIndexQuery;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.TriFunction;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OrientGraphStep<S, E extends Element> extends GraphStep<S, E> implements HasContainerHolder {

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
    private <ElementType extends Element> Iterator<? extends ElementType> elements(
            BiFunction<OrientGraph, Object[], Iterator<ElementType>> getElementsByIds,
            TriFunction<OrientGraph, OIndex<Object>, Iterator<Object>, Stream<? extends ElementType>> getElementsByIndex,
            Function<OrientGraph, Iterator<ElementType>> getAllElements) {
        final OrientGraph graph = getGraph();

        if (this.ids != null && this.ids.length > 0) {
            /** Got some element IDs, so just get the elements using those */
            return this.iteratorList(getElementsByIds.apply(graph, this.ids));
        } else {
            /** Have no element IDs. See if there's an indexed property to use */
            Set<OrientIndexQuery> indexQueryOptions = findIndex();

            if (!indexQueryOptions.isEmpty()) {
                List<ElementType> elements = new ArrayList<>();

                indexQueryOptions.forEach(indexQuery -> {
                    OLogManager.instance().debug(this, "using " + indexQuery);
                    Stream<? extends ElementType> indexedElements = getElementsByIndex.apply(graph, indexQuery.index, indexQuery.values);
                    elements.addAll(indexedElements.filter(element -> HasContainer.testAll(element, this.hasContainers))
                            .collect(Collectors.<ElementType> toList()));
                });

                return elements.iterator();
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

    // if one of the HasContainers is a label matching predicate, then return those labels
    private Set<String> findClassLabelsInHasContainers() {
        Set<String> classLabels = new HashSet<>();

        Optional<HasContainer> container = this.hasContainers.stream()
                .filter(hasContainer -> isLabelKey(hasContainer.getKey()))
                .findFirst();

        if (container.isPresent()) {
            Object value = container.get().getValue();

            //The ugly part. Is there anyway to know the return type of a predicate value ?
            if (value instanceof List) {
                ((List) value).forEach(label -> classLabels.add((String) label));
            } else {
                classLabels.add((String) value);
            }
        }

        return classLabels;
    }

    private OrientGraph getGraph() {
        return (OrientGraph) this.getTraversal().getGraph().get();
    }

    @VisibleForTesting
    public Set<OrientIndexQuery> findIndex() {
        final Set<OrientIndexQuery> indexedQueries = new HashSet<>();
        final OrientGraph graph = getGraph();
        final OIndexManagerProxy indexManager = graph.database().getMetadata().getIndexManager();

        // find indexed keys only for the element subclasses (if present)
        final Set<String> classLabels = findClassLabelsInHasContainers();

        if (!classLabels.isEmpty()) {
            final Set<String> indexedKeys = new HashSet<>();// classLabels.isPresent() ? graph.getIndexedKeys(this.returnClass, classLabels.get()) : graph.getIndexedKeys(this.returnClass);

            classLabels.forEach(label -> indexedKeys.addAll(graph.getIndexedKeys(this.returnClass, label)));

            Optional<Pair<String, Iterator<Object>>> requestedKeyValue = this.hasContainers.stream()
                    .filter(c -> indexedKeys.contains(c.getKey()) && (c.getPredicate().getBiPredicate() == Compare.eq ||
                            c.getPredicate().getBiPredicate() == Contains.within))
                    .findAny()
                    .map(c -> getValuePair(c))
                    .orElseGet(Optional::empty);

            if (requestedKeyValue.isPresent()) {
                String key = requestedKeyValue.get().getValue0();
                Iterator<Object> values = requestedKeyValue.get().getValue1();

                classLabels.forEach(classLabel -> {
                    String className = graph.labelToClassName(classLabel, isVertexStep() ? OImmutableClass.VERTEX_CLASS_NAME : OImmutableClass.EDGE_CLASS_NAME);
                    Set<OIndex<?>> classIndexes = indexManager.getClassIndexes(className);
                    Iterator<OIndex<?>> keyIndexes = classIndexes.stream().filter(idx -> idx.getDefinition().getFields().contains(key)).iterator();

                    if (keyIndexes.hasNext()) {
                        // TODO: select best index if there are multiple options
                        indexedQueries.add(new OrientIndexQuery(keyIndexes.next(), values));
                    } else {
                        OLogManager.instance().warn(this, "no index found for class=[" + className + "] and key=[" + key + "]");
                    }

                });

                return indexedQueries;
            }
        }

        return indexedQueries;
    }

    /** gets the requested values from the Has step. If it's a single value, wrap it in an array, otherwise return the array
     *  */
    private Optional<Pair<String, Iterator<Object>>> getValuePair(HasContainer c) {
        Iterator<Object> values;
        if (c.getPredicate().getBiPredicate() == Contains.within)
            values = ((Iterable<Object>) c.getValue()).iterator();
        else
            values = IteratorUtils.of(c.getValue());
        return Optional.of(new Pair<>(c.getKey(), values));

    }

    @Override
    public String toString() {
        if (this.hasContainers.isEmpty())
            return super.toString();
        else
            return 0 == this.ids.length ? StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), this.hasContainers)
                    : StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), Arrays.toString(this.ids), this.hasContainers);
    }

	private <X extends Element> Iterator<X> iteratorList(final Iterator<X> iterator) {
		final List<X> list = new ArrayList<>();
        while (iterator.hasNext()) {
			final X e = iterator.next();
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
