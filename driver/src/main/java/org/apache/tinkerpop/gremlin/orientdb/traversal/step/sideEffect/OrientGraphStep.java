package org.apache.tinkerpop.gremlin.orientdb.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.orientdb.OrientIndexReference;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientVertex;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.javatuples.Pair;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OrientGraphStep<S extends Element> extends GraphStep<S> implements HasContainerHolder {

    private final List<HasContainer> hasContainers = new ArrayList<>();

    public OrientGraphStep(final GraphStep<S> originalGraphStep) {
        super(originalGraphStep.getTraversal(), originalGraphStep.getReturnClass(), originalGraphStep.getIds());
        originalGraphStep.getLabels().forEach(this::addLabel);
        this.setIteratorSupplier(() -> (Iterator<S>) (isVertexStep() ? this.vertices() : this.edges()));
    }

    private boolean isVertexStep() {
        return Vertex.class.isAssignableFrom(this.returnClass);
    }

    private Iterator<? extends Vertex> vertices() {
        final OrientGraph graph = (OrientGraph) this.getTraversal().getGraph().get();
        final Optional<OrientIndexReference> indexReference = getIndexReference();

        if (this.ids != null && this.ids.length > 0) {
            return this.iteratorList(graph.vertices(this.ids));
        } else if (!indexReference.isPresent()) {
//            System.out.println("not indexed");
            return this.iteratorList(graph.vertices());
        } else {
//            System.out.println("index will be queried with " + indexReference.get());
            Stream<OrientVertex> indexedVertices = graph.getIndexedVertices(indexReference.get());
            return indexedVertices
                .filter(vertex -> HasContainer.testAll(vertex, this.hasContainers))
                .collect(Collectors.<Vertex>toList())
                .iterator();
        }
    }

    //TODO: indexed edges
    private Iterator<? extends Edge> edges() {
        final OrientGraph graph = (OrientGraph) this.getTraversal().getGraph().get();
        if (this.ids != null && this.ids.length > 0) {
            return this.iteratorList(graph.edges(this.ids));
        } else {
            return this.iteratorList(graph.edges());
        }
    }

    // if one of the HasContainers is a label matching predicate, then return that label
    private Optional<String> findElementLabelInHasContainers() {
        return this.hasContainers.stream()
            .filter(hasContainer -> T.fromString(hasContainer.getKey()) == T.label)
            .findFirst()
            .map(hasContainer -> hasContainer.getValue().toString());
    }

    private OrientGraph getGraph() {
        return ((OrientGraph) this.getTraversal().getGraph().get());
    }

    private Optional<OrientIndexReference> getIndexReference() {
        Optional<String> elementLabel = findElementLabelInHasContainers();
        OrientGraph graph = getGraph();
        // find indexed keys only for the element subclass (if present)
        final Set<String> indexedKeys = elementLabel.isPresent() ?
            graph.getIndexedKeys(this.returnClass, elementLabel.get()) :
            graph.getIndexedKeys(this.returnClass);

        Optional<Pair<String, Object>> indexedKeyAndValue =
            this.hasContainers.stream()
            .filter(c -> indexedKeys.contains(c.getKey()) && c.getPredicate().getBiPredicate() == Compare.eq)
            .findAny()
            .map(c -> Optional.of(new Pair<>(c.getKey(), c.getValue())))
            .orElseGet(Optional::empty);

        if (indexedKeyAndValue.isPresent()) {
            String key = indexedKeyAndValue.get().getValue0();
            Object value = indexedKeyAndValue.get().getValue1();
            return Optional.of(new OrientIndexReference(isVertexStep(), elementLabel, key, value));
        } else
            return Optional.empty();
    }

    @Override
    public String toString() {
        if (this.hasContainers.isEmpty())
            return super.toString();
        else
            return 0 == this.ids.length ?
                StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), this.hasContainers) :
                StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), Arrays.toString(this.ids), this.hasContainers);
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
