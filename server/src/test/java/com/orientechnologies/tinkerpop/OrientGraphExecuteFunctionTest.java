package com.orientechnologies.tinkerpop;

import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibrary;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.StreamUtils;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Enrico Risa on 14/11/16.
 */
public class OrientGraphExecuteFunctionTest extends AbstractRemoteTest {

  @Test
  public void testExecuteGremlinSimpleFunctionTest() {

    OrientGraph noTx = factory.getNoTx();

    noTx.addVertex(T.label, "Person", "name", "John");
    noTx.addVertex(T.label, "Person", "name", "Luke");

    OFunctionLibrary functionLibrary = noTx.getRawDatabase().getMetadata().getFunctionLibrary();

    OFunction testGremlin = functionLibrary.createFunction("testGremlin");
    testGremlin.setLanguage("gremlin-groovy");
    testGremlin.setCode("g.V()");

    testGremlin.save();

    Iterator gremlin = (Iterator) testGremlin.execute();

    Assert.assertEquals(2, StreamUtils.asStream(gremlin).count());

  }

  @Test
  public void testExecuteGremlinFunctionCountQueryTest() {

    OrientGraph noTx = factory.getNoTx();

    noTx.addVertex(T.label, "Person", "name", "John");
    noTx.addVertex(T.label, "Person", "name", "Luke");

    OFunctionLibrary functionLibrary = noTx.getRawDatabase().getMetadata().getFunctionLibrary();

    OFunction testGremlin = functionLibrary.createFunction("testGremlin");
    testGremlin.setLanguage("gremlin-groovy");
    testGremlin.setCode("g.V().count()");

    testGremlin.save();

    Iterator gremlin = (Iterator) testGremlin.execute();

    Assert.assertEquals(true, gremlin.hasNext());
    Object result = gremlin.next();
    Assert.assertEquals(new Long(2), result);

  }

  // Still Not supported
  @Test
  @Ignore
  public void testExecuteGremlinSqlFunctionInvokeTest() {

    OrientGraph noTx = factory.getNoTx();

    noTx.addVertex(T.label, "Person", "name", "John");
    noTx.addVertex(T.label, "Person", "name", "Luke");

    OFunctionLibrary functionLibrary = noTx.getRawDatabase().getMetadata().getFunctionLibrary();

    OFunction testGremlin = functionLibrary.createFunction("testGremlin");
    testGremlin.setLanguage("gremlin-groovy");
    testGremlin.setCode("g.V()");

    testGremlin.save();

    OResultSet gremlin = noTx.executeSql("select testGremlin() as gremlin");

    Assert.assertEquals(true, gremlin.hasNext());

    OResult result = gremlin.next();

    Collection value = result.getProperty("gremlin");

    Assert.assertEquals(2, value.size());
  }



  @Test
  public void testExecuteGremlinSqlExpandFunctionInvokeTest() {

    OrientGraph noTx = factory.getNoTx();

    noTx.addVertex(T.label, "Person", "name", "John");
    noTx.addVertex(T.label, "Person", "name", "Luke");

    OFunctionLibrary functionLibrary = noTx.getRawDatabase().getMetadata().getFunctionLibrary();

    OFunction testGremlin = functionLibrary.createFunction("testGremlin");
    testGremlin.setLanguage("gremlin-groovy");
    testGremlin.setCode("g.V()");

    testGremlin.save();

    OResultSet gremlin = noTx.executeSql("select expand(testGremlin())");

    List<OResult> collect = gremlin.stream().collect(Collectors.toList());

    Assert.assertEquals(2, collect.size());

    collect.stream().forEach((res) -> {
      Assert.assertEquals(true, res.isVertex());

      OVertex oVertex = res.getVertex().get();

      Assert.assertEquals("Person", oVertex.getSchemaType().get().getName());
    });

  }
}
