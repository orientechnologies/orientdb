package org.apache.tinkerpop.gremlin.orientdb.traversal.step.map;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.tinkerpop.gremlin.orientdb.OGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;

/** @author Enrico Risa */
public class OrientClassCountStep<S> extends AbstractStep<S, Long> {

  private final boolean vertexStep;
  private List<String> klasses;

  protected boolean done = false;

  public OrientClassCountStep(Traversal.Admin traversal, List<String> klasses, boolean vertexStep) {
    super(traversal);
    this.klasses = klasses;
    this.vertexStep = vertexStep;
  }

  @Override
  protected Traverser.Admin<Long> processNextStart() throws NoSuchElementException {
    if (!done) {
      done = true;
      ODatabaseDocument db = getDatabase();
      Long v =
          klasses.stream()
              .filter(this::filterClass)
              .mapToLong((klass) -> db.countClass(klass))
              .reduce(0, (a, b) -> a + b);
      return this.traversal.getTraverserGenerator().generate(v, (Step) this, 1L);
    } else {
      throw FastNoSuchElementException.instance();
    }
  }

  private ODatabaseDocument getDatabase() {
    OGraph graph = (OGraph) this.traversal.getGraph().get();
    return graph.getRawDatabase();
  }

  private boolean filterClass(String klass) {

    ODatabaseDocument database = getDatabase();
    OSchema schema = database.getMetadata().getSchema();
    OClass schemaClass = schema.getClass(klass);

    if (vertexStep) {
      return schemaClass.isSubClassOf("V");
    } else {
      return schemaClass.isSubClassOf("E");
    }
  }

  public List<String> getKlasses() {
    return klasses;
  }
}
