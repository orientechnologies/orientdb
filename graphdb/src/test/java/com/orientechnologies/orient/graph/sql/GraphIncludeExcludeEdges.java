/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *  
 */

package com.orientechnologies.orient.graph.sql;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.graph.GraphNoTxAbstractTest;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Luca Garulli
 */
public class GraphIncludeExcludeEdges extends GraphNoTxAbstractTest {
  private final int TOT = 10;

  @Test
  public void testIntersect() {
    graph.setUseLightweightEdges(false);

    // CREATE SUPER NODE1
    final OrientVertex root1 = graph.addVertex(null, "name", "root1");
    for (int i = 0; i < TOT; ++i) {
      root1.addEdge("E", graph.addVertex(null, "child", i));
    }
    graph.commit();

    final Iterable<ODocument> result = graph.getRawGraph().command(new OCommandSQL("select @this.exclude('out_*','in_*') from V"))
        .execute();

    Assert.assertTrue(result.iterator().hasNext());
    for (ODocument v : result) {
      for (String f : v.fieldNames()) {
        Assert.assertFalse(f.startsWith("out_"));
        Assert.assertFalse(f.startsWith("in_"));
      }
    }
  }

  @BeforeClass
  public static void init() {
    System.setProperty("orientdb.test.env", "ci");
    init(GraphIncludeExcludeEdges.class.getSimpleName());
  }
}
