/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tinkerpop.gremlin.orientdb.executor;

import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibrary;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphBaseTest;
import org.apache.tinkerpop.gremlin.orientdb.OrientVertex;
import org.apache.tinkerpop.gremlin.orientdb.StreamUtils;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Assert;
import org.junit.Test;

/** Created by Enrico Risa on 14/11/16. */
public class OrientGraphExecuteFunctionTest extends OrientGraphBaseTest {

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

  @Test
  public void testExecuteGremlinSqlFunctionInvokeTest() {

    OrientGraph noTx = factory.getNoTx();

    noTx.addVertex(T.label, "Person", "name", "John");
    noTx.addVertex(T.label, "Person", "name", "Luke");

    OFunctionLibrary functionLibrary = noTx.getRawDatabase().getMetadata().getFunctionLibrary();

    OFunction testGremlin = functionLibrary.createFunction("testGremlin");
    testGremlin.setLanguage("gremlin-groovy");
    testGremlin.setCode("g.V()");

    testGremlin.save();

    OGremlinResultSet gremlin = noTx.executeSql("select testGremlin() as gremlin");

    Iterator<OGremlinResult> iterator = gremlin.iterator();
    Assert.assertEquals(true, iterator.hasNext());

    OGremlinResult result = iterator.next();

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

    OGremlinResultSet gremlin = noTx.executeSql("select expand(testGremlin())");

    List<OGremlinResult> collect = gremlin.stream().collect(Collectors.toList());

    Assert.assertEquals(2, collect.size());

    collect.stream()
        .forEach(
            (res) -> {
              Assert.assertEquals(true, res.isVertex());

              OrientVertex oVertex = res.getVertex().get();

              Assert.assertEquals(
                  "Person", oVertex.getRawElement().getSchemaType().get().getName());
            });
  }
}
