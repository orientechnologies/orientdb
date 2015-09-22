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

import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.graph.GraphNoTxAbstractTest;
import com.tinkerpop.blueprints.Vertex;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class GraphCommands extends GraphNoTxAbstractTest {

  @Test
  public void testEmptyParams() {
    String sql = "SELECT FROM V WHERE tags NOT IN :tags";
    Map<String, Object> queryParams = new HashMap<String, Object>();
    queryParams.put("tags", new HashSet<String>());

    Iterable<Vertex> results = ((Iterable<Vertex>) graph.command(new OSQLSynchQuery(sql)).execute(queryParams));
    Assert.assertTrue(results.iterator().hasNext());
  }

  @Test
  public void testParams() {
    String sql = "SELECT FROM V WHERE tags IN :tags";
    Map<String, Object> queryParams = new HashMap<String, Object>();
    queryParams.put("tags", new HashSet<String>() {
      {
        add("Genius");
      }
    });

    Iterable<Vertex> results = ((Iterable<Vertex>) graph.command(new OSQLSynchQuery(sql)).execute(queryParams));
    Assert.assertTrue(results.iterator().hasNext());

    sql = "SELECT FROM V WHERE tags NOT IN :tags";
    queryParams = new HashMap<String, Object>();
    queryParams.put("tags", new HashSet<String>() {
      {
        add("Genius");
      }
    });

    results = ((Iterable<Vertex>) graph.command(new OSQLSynchQuery(sql)).execute(queryParams));
    Assert.assertFalse(results.iterator().hasNext());
  }

  @Test
  public void testAddValueSQL() {
    graph.command(new OCommandSQL("update V add testprop = 'first' return after @this limit 1")).execute();

    Iterable<Vertex> results = ((Iterable<Vertex>) graph.command(
        new OSQLSynchQuery("select from V where 'first' in testprop")).execute());
    Assert.assertTrue(results.iterator().hasNext());

    graph.command(new OCommandSQL("update V add testprop = 'second' return after @this limit 1")).execute();

    results = ((Iterable<Vertex>) graph.command(new OSQLSynchQuery("select from V where 'first' in testprop")).execute());
    Assert.assertTrue(results.iterator().hasNext());
    results = ((Iterable<Vertex>) graph.command(new OSQLSynchQuery("select from V where 'second' in testprop")).execute());
    Assert.assertTrue(results.iterator().hasNext());
  }

  @BeforeClass
  public static void init() {
    init(SQLGraphBatchTest.class.getSimpleName());

    final List<String> names = new ArrayList<String>();
    names.add("Genius");
    graph.addVertex(null).setProperty("tags", names);
    graph.commit();
  }
}
