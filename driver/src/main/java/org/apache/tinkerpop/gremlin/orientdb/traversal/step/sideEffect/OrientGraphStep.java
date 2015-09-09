package org.apache.tinkerpop.gremlin.orientdb.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientVertex;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OrientGraphStep<S extends Element> extends GraphStep<S> implements HasContainerHolder {

    private final List<HasContainer> hasContainers = new ArrayList<>();

    public OrientGraphStep(final GraphStep<S> originalGraphStep) {
        super(originalGraphStep.getTraversal(), originalGraphStep.getReturnClass(), originalGraphStep.getIds());
        originalGraphStep.getLabels().forEach(this::addLabel);
        this.setIteratorSupplier(() -> (Iterator<S>) (Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges()));
    }

    private Iterator<? extends Vertex> vertices() {
        final OrientGraph graph = (OrientGraph) this.getTraversal().getGraph().get();
        final HasContainer indexedContainer = getIndexKey(this.returnClass);

        if (this.ids != null && this.ids.length > 0) {
            return this.iteratorList(graph.vertices(this.ids));
        }
        else if (indexedContainer == null) {
//            System.out.println("not indexed");
            return this.iteratorList(graph.vertices());
        } else {
//            System.out.println("indexed");
            Stream<OrientVertex> indexedVertices = graph.getIndexedVertices(indexedContainer.getKey(), indexedContainer.getValue());
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

    private HasContainer getIndexKey(final Class<? extends Element> indexedClass) {
        final Set<String> indexedKeys = ((OrientGraph) this.getTraversal().getGraph().get()).getIndexedKeys(indexedClass);
        return this.hasContainers.stream()
            .filter(c -> indexedKeys.contains(c.getKey()) && c.getPredicate().getBiPredicate() == Compare.eq)
            .findAny()
            .orElseGet(() -> null);
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
