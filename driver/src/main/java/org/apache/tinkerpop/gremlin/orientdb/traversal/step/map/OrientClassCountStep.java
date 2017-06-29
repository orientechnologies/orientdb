package org.apache.tinkerpop.gremlin.orientdb.traversal.step.map;

import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.NoSuchElementException;

/**
 * Created by Enrico Risa on 27/06/2017.
 */
public class OrientClassCountStep<S> extends AbstractStep<S, Long> {

    private String klass;

    protected boolean done = false;

    public OrientClassCountStep(Traversal.Admin traversal, GraphStep prev) {
        this(traversal, "");
        klass = baseClass(prev);
    }

    public OrientClassCountStep(Traversal.Admin traversal, String klass) {
        super(traversal);
        this.klass = klass;
    }

    protected String baseClass(GraphStep step) {
        return Vertex.class.isAssignableFrom(step.getReturnClass()) ? "V" : "E";
    }

    @Override
    protected Traverser.Admin<Long> processNextStart() throws NoSuchElementException {
        if (!done) {
            done = true;
            OrientGraph graph = (OrientGraph) this.traversal.getGraph().get();
            Long v = graph.getRawDatabase().countClass(this.klass);
            return this.traversal.getTraverserGenerator().generate(v, (Step) this, 1L);
        } else {
            throw FastNoSuchElementException.instance();
        }
    }

    public String getKlass() {
        return klass;
    }
}
