/*
 *
 *  * Copyright 2010-2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.orient.graph.blueprints;

import static org.junit.Assert.fail;

import org.junit.Test;

import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;

public class GraphTests {

  public static final String URL = "plocal:/target/databases/testExceptionOnCommit";

  @Test
  public void indexes() {
    OrientGraph g = new OrientGraph(URL, "admin", "admin");
    try {
      if (g.getVertexType("VC1") == null) {
        g.createVertexType("VC1");
      }
    } finally {
      g.shutdown();
    }
    g = new OrientGraph(URL, "admin", "admin");
    try {
      // System.out.println(g.getIndexedKeys(Vertex.class,true)); this will print VC1.p1
      if (g.getIndex("VC1.p1", Vertex.class) == null) {// this will return null. I do not know why
        g.createKeyIndex("p1", Vertex.class, new Parameter<String, String>("class", "VC1"), new Parameter<String, String>("type",
            "UNIQUE"), new Parameter<String, OType>("keytype", OType.STRING));
      }
    } catch (OIndexException e) {
      // ignore because the index may exist
    } finally {
      g.shutdown();
    }

    g = new OrientGraph(URL, "admin", "admin");
    String val1 = System.currentTimeMillis() + "";
    try {
      Vertex v = g.addVertex("class:VC1");
      v.setProperty("p1", val1);
    } finally {
      g.shutdown();
    }
    g = new OrientGraph(URL, "admin", "admin");
    try {
      Vertex v = g.addVertex("class:VC1");
      v.setProperty("p1", val1);
    } finally {
      try {
        g.shutdown();
        fail("must throw duplicate key here!");
      } catch (ORecordDuplicatedException e) {
        // ok
      }

    }
  }
}
