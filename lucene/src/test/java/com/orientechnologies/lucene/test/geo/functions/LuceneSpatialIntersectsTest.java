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
public class LuceneSpatialIntersectsTest {

  @Test
  public void testIntersectsNoIndex() {

    OrientGraphNoTx graph = new OrientGraphNoTx("memory:functionsTest");
    try {
      ODatabaseDocumentTx db = graph.getRawGraph();

      List<ODocument> execute = db.command(new OCommandSQL("SELECT ST_Intersects('POINT(0 0)', 'LINESTRING ( 2 0, 0 2 )')"))
          .execute();
      ODocument next = execute.iterator().next();

      Assert.assertEquals(next.field("ST_Intersects"), false);
      execute = db.command(new OCommandSQL("SELECT ST_Intersects('POINT(0 0)', 'LINESTRING ( 0 0, 0 2 )')")).execute();
      next = execute.iterator().next();

      Assert.assertEquals(next.field("ST_Intersects"), true);

    } finally {
      graph.drop();
    }
  }

  @Test
  public void testIntersectsIndex() {

    OrientGraphNoTx graph = new OrientGraphNoTx("memory:functionsTest");
    try {
      ODatabaseDocumentTx db = graph.getRawGraph();

      db.command(new OCommandSQL("create class Lines extends v")).execute();
      db.command(new OCommandSQL("create property Lines.geometry EMBEDDED OLINESTRING")).execute();

      db.command(new OCommandSQL("insert into Lines set geometry = ST_GeomFromText('LINESTRING ( 2 0, 0 2 )')")).execute();
      db.command(new OCommandSQL("insert into Lines set geometry = ST_GeomFromText('LINESTRING ( 0 0, 0 2 )')")).execute();

      db.command(new OCommandSQL("create index L.g on Lines (geometry) SPATIAL engine lucene")).execute();
      List<ODocument> execute = db.command(new OCommandSQL("SELECT from lines where ST_Intersects(geometry, 'POINT(0 0)') = true"))
          .execute();

      Assert.assertEquals(execute.size(), 1);

    } finally {
      graph.drop();
    }
  }

}
