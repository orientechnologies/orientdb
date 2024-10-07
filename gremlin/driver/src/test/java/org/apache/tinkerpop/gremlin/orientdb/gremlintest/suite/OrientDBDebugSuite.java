package org.apache.tinkerpop.gremlin.orientdb.gremlintest.suite;

import org.apache.tinkerpop.gremlin.AbstractGremlinSuite;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.structure.io.IoCustomTest;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

/** Created by Enrico Risa on 10/11/16. */
public class OrientDBDebugSuite extends AbstractGremlinSuite {

  private static final Class<?>[] allTests =
      new Class<?>[] {
        IoCustomTest.class,
        //      IoVertexTest.class
        //            GraphTest.class,
        //            TransactionTest.class,
        //            VertexTest.class
        //            TransactionTest.class
      };

  public OrientDBDebugSuite(final Class<?> klass, final RunnerBuilder builder)
      throws InitializationError {
    super(klass, builder, allTests, null, false, TraversalEngine.Type.STANDARD);
  }
}
