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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.graph.GraphNoTxAbstractTest;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collection;

public class GraphIntersectLightweightEdges extends GraphNoTxAbstractTest {
  private final int TOT = 1000;

  @Test
  public void testIntersect() {

    graph.setUseLightweightEdges(true);

    graph.setAutoStartTx(false);
    graph.declareIntent(new OIntentMassiveInsert().setDisableSecurity(true).setDisableHooks(true).setDisableValidation(true)
        .setEnableCache(false));

    // graph.begin();

    // CREATE SUPER NODE1
    final OrientVertex root1 = graph.addVertex(null, "name", "root1");
    for (int i = 0; i < TOT; ++i) {
      root1.addEdge("E", graph.addVertex(null, "child", i));
      if (i % 1000 == 0) {
        graph.commit();
        graph.setUseLog(false);
        // graph.begin();

        System.out.println("commit " + i);
      }
    }

    OLogManager.instance().info(this, "Created root1 with " + TOT + " children");

    // CREATE SUPER NODE2
    final OrientVertex root2 = graph.addVertex(null, "name", "root2");
    for (int i = 0; i < TOT; ++i) {
      root2.addEdge("E", graph.addVertex(null, "child", i));
      if (i % 1000 == 0) {
        graph.commit();
        graph.setUseLog(false);
        // graph.begin();

        System.out.println("commit " + i);
      }
    }

    OLogManager.instance().info(this, "Created root2 with " + TOT + " children");

    // CREATE THE VERTEX IN COMMON
    final OrientVertex common = graph.addVertex(null, "common", true);

    root1.addEdge("E", common);
    root2.addEdge("E", common);

    graph.commit();

    OLogManager.instance().info(this, "Intersecting...");

    Iterable<OrientVertex> result = graph.command(new OCommandSQL("select intersect( out() ) from [?,?]")).execute(
        root1.getIdentity(), root2.getIdentity());

    OLogManager.instance().info(this, "Intersecting done");

    Assert.assertTrue(result.iterator().hasNext());
    OrientVertex o = result.iterator().next();

    final Collection set = o.getRecord().field("intersect");

    Assert.assertEquals(set.iterator().next(), common);

    result = graph.command(
        new OCommandSQL(
            "select expand($c) let $a=(select from V limit 20), $b=(select from V skip 10 limit 10), $c=intersect( $a, $b )"))
        .execute();

    Assert.assertTrue(result.iterator().hasNext());
  }

  @BeforeClass
  public static void init() {
    // System.setProperty("orientdb.test.env", "ci");
    init(GraphIntersectLightweightEdges.class.getSimpleName());
  }
}
