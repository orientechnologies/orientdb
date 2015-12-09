/*
 *
 *  * Copyright 2014 Orient Technologies.
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

package com.orientechnologies.lucene.test.geo.functions;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Created by Enrico Risa on 28/09/15.
 */
@Test(groups = "embedded")
public class LuceneSpatialDWithinTest {

  @Test
  public void testDWithinNoIndex() {

    OrientGraphNoTx graph = new OrientGraphNoTx("memory:functionsTest");
    try {
      ODatabaseDocumentTx db = graph.getRawGraph();

      List<ODocument> execute = db.command(
          new OCommandSQL("SELECT ST_DWithin(ST_GeomFromText('POLYGON((0 0, 10 0, 10 5, 0 5, 0 0))'), "
              + "ST_GeomFromText('POLYGON((12 0, 14 0, 14 6, 12 6, 12 0))'), 2.0d) as distance")).execute();
      ODocument next = execute.iterator().next();

      Assert.assertEquals(next.field("distance"), true);

    } finally {
      graph.drop();
    }
  }

  @Test(enabled = false)
  public void testWithinIndex() {

    OrientGraphNoTx graph = new OrientGraphNoTx("memory:functionsTest");
    try {
      ODatabaseDocumentTx db = graph.getRawGraph();

      db.command(new OCommandSQL("create class Polygon extends v")).execute();
      db.command(new OCommandSQL("create property Polygon.geometry EMBEDDED OPolygon")).execute();

      db.command(new OCommandSQL("insert into Polygon set geometry = ST_GeomFromText('POLYGON((0 0, 10 0, 10 5, 0 5, 0 0))')"))
          .execute();

      db.command(new OCommandSQL("create index Polygon.g on Polygon (geometry) SPATIAL engine lucene")).execute();
      List<ODocument> execute = db
          .command(
              new OCommandSQL(
                  "SELECT from Polygon where ST_DWithin(geometry, ST_GeomFromText('POLYGON((12 0, 14 0, 14 6, 12 6, 12 0))'), 2.0d) = true"))
          .execute();

      Assert.assertEquals(execute.size(), 1);

      // execute = db.command(
      // new OCommandSQL("SELECT from Polygon where ST_Within(geometry, ST_Buffer(ST_GeomFromText('POINT(50 50)'), 30)) = true"))
      // .execute();

      // Assert.assertEquals(execute.size(), 1);

    } finally {
      graph.drop();
    }
  }

}
