/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

package com.orientechnologies.lucene.test;

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;

/**
 * Created by enricorisa on 28/06/14.
 */

public class LuceneGraphTXTest {

  @Test
  public void graphTxTest() throws Exception {

    OrientGraph graph = new OrientGraph("memory:graphTx");

    try {

      graph.executeOutsideTx(new OCallable<Object, OrientBaseGraph>() {
        @Override
        public Object call(OrientBaseGraph graph) {
          OrientVertexType city = graph.createVertexType("City");
          city.createProperty("name", OType.STRING);
          graph.command(new OCommandSQL("create index City.name on City (name) FULLTEXT ENGINE LUCENE")).execute();
          return null;
        }
      });

      OrientVertex v = graph.addVertex("class:City", "name", "London");

      v.save();

      Collection results = graph.getRawGraph().command(new OCommandSQL("select from City where name lucene 'London'")).execute();
      Assert.assertEquals(results.size(), 1);

      v.setProperty("name", "Berlin");

      v.save();

      results = graph.getRawGraph().command(new OCommandSQL("select from City where name lucene 'Berlin'")).execute();
      Assert.assertEquals(results.size(), 1);

      results = graph.getRawGraph().command(new OCommandSQL("select from City where name lucene 'London'")).execute();
      Assert.assertEquals(results.size(), 0);

      graph.commit();

      // Assert After Commit
      results = graph.getRawGraph().command(new OCommandSQL("select from City where name lucene 'Berlin'")).execute();
      Assert.assertEquals(results.size(), 1);
      results = graph.getRawGraph().command(new OCommandSQL("select from City where name lucene 'London'")).execute();
      Assert.assertEquals(results.size(), 0);

    } finally {
      graph.drop();
    }

  }

}
