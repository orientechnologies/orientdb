package org.apache.tinkerpop.gremlin.orientdb;

import java.util.Optional;
import org.apache.tinkerpop.gremlin.orientdb.traversal.step.sideeffect.OrientGraphStep;
import org.apache.tinkerpop.gremlin.orientdb.traversal.strategy.optimization.OrientGraphStepStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

/** Created by Enrico Risa on 14/11/16. */
public abstract class OrientGraphBaseTest {

  @Rule public TestName name = new TestName();

  protected OrientGraphFactory factory;

  @Before
  public void setupDB() {
    String config = System.getProperty("orientdb.test.env", "memory");

    String url = null;
    if ("ci".equals(config) || "release".equals(config)) {
      url = "plocal:./target/databases/" + name.getMethodName();
    } else {
      url = "memory:" + name.getMethodName();
    }
    factory = new OrientGraphFactory(url);
  }

  @After
  public void dropDB() {

    factory.drop();
  }

  protected int usedIndexes(OrientGraph graph, GraphTraversal<?, ?> traversal) {
    OrientGraphStepStrategy.instance().apply(traversal.asAdmin());
    OrientGraphStep orientGraphStep = (OrientGraphStep) traversal.asAdmin().getStartStep();
    Optional<OrientGraphQuery> optional = orientGraphStep.buildQuery();
    Optional<Integer> index = optional.map(query -> query.usedIndexes(graph));
    return index.isPresent() ? index.get() : 0;
  }
}
