package com.orientechnologies.tinkerpop;

import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by Enrico Risa on 26/01/17.
 */
public class OrientGraphExecuteQueryRemoteTest extends AbstractRemoteTest {


  @Test
  public void testExecuteGremlinSimpleQueryTest() {

    OrientGraph noTx = factory.getNoTx();

    noTx.addVertex(T.label, "Person", "name", "John");
    noTx.addVertex(T.label, "Person", "name", "Luke");

    OResultSet gremlin = noTx.execute("gremlin", "g.V()", null);

    Assert.assertEquals(2, gremlin.stream().count());
  }


  @Test
  public void testExecuteGremlinCountQueryTest() {

    OrientGraph noTx = factory.getNoTx();

    noTx.addVertex(T.label, "Person", "name", "John");
    noTx.addVertex(T.label, "Person", "name", "Luke");

    OResultSet gremlin = noTx.execute("gremlin", "g.V().count()", null);

    Assert.assertEquals(true, gremlin.hasNext());
    OResult result = gremlin.next();
    Long count = result.getProperty("value");
    Assert.assertEquals(new Long(2), count);

  }
}
