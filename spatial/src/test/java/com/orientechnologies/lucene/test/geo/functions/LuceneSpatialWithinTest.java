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
public class LuceneSpatialWithinTest {

  @Test
  public void testWithinNoIndex() {

    OrientGraphNoTx graph = new OrientGraphNoTx("memory:functionsTest");
    try {
      ODatabaseDocumentTx db = graph.getRawGraph();

      List<ODocument> execute = db
          .command(
              new OCommandSQL(
                  "select ST_Within(smallc,smallc) as smallinsmall,ST_Within(smallc, bigc) As smallinbig, ST_Within(bigc,smallc) As biginsmall "
                  + "from (SELECT ST_Buffer(ST_GeomFromText('POINT(50 50)'), 20) As smallc,ST_Buffer(ST_GeomFromText('POINT(50 50)'), 40) As bigc)"))
          .execute();
      ODocument next = execute.iterator()
                              .next();

      Assert.assertEquals(next.field("smallinsmall"), false);
      Assert.assertEquals(next.field("smallinbig"), true);
      Assert.assertEquals(next.field("biginsmall"), false);

    } finally {
      graph.drop();
    }
  }

  @Test
  public void testWithinIndex() {

    OrientGraphNoTx graph = new OrientGraphNoTx("memory:functionsTest");
    try {
      ODatabaseDocumentTx db = graph.getRawGraph();

      db.command(new OCommandSQL("create class Polygon extends v"))
        .execute();
      db.command(new OCommandSQL("create property Polygon.geometry EMBEDDED OPolygon"))
        .execute();

      db.command(new OCommandSQL("insert into Polygon set geometry = ST_Buffer(ST_GeomFromText('POINT(50 50)'), 20)"))
        .execute();
      db.command(new OCommandSQL("insert into Polygon set geometry = ST_Buffer(ST_GeomFromText('POINT(50 50)'), 40)"))
        .execute();

      db.command(new OCommandSQL("create index Polygon.g on Polygon (geometry) SPATIAL engine lucene"))
        .execute();
      List<ODocument> execute = db.command(
          new OCommandSQL("SELECT from Polygon where ST_Within(geometry, ST_Buffer(ST_GeomFromText('POINT(50 50)'), 50)) = true"))
                                  .execute();

      Assert.assertEquals(execute.size(), 2);

      execute = db.command(
          new OCommandSQL("SELECT from Polygon where ST_Within(geometry, ST_Buffer(ST_GeomFromText('POINT(50 50)'), 30)) = true"))
                  .execute();

      Assert.assertEquals(execute.size(), 1);

    } finally {
      graph.drop();
    }
  }

}
