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

package com.orientechnologies.lucene.test.geo;

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
public class LuceneSpatialMiscFunctionsTest {

  @Test
  public void testStEquals() {

    OrientGraphNoTx graph = new OrientGraphNoTx("memory:functionsTest");
    try {
      ODatabaseDocumentTx db = graph.getRawGraph();

      List<ODocument> execute = db.command(
          new OCommandSQL(
              "SELECT ST_Equals(ST_GeomFromText('LINESTRING(0 0, 10 10)'), ST_GeomFromText('LINESTRING(0 0, 5 5, 10 10)'))"))
          .execute();
      ODocument next = execute.iterator().next();
      Assert.assertEquals(next.field("ST_Equals"), true);
    } finally {
      graph.drop();
    }
  }

  @Test
  public void testAsBinary() {

    OrientGraphNoTx graph = new OrientGraphNoTx("memory:functionsTest");
    try {
      ODatabaseDocumentTx db = graph.getRawGraph();

      List<ODocument> execute = db.command(new OCommandSQL("SELECT ST_AsBinary(ST_GeomFromText('LINESTRING(0 0, 10 10)'))"))
          .execute();
      ODocument next = execute.iterator().next();
      // TODO CHANGE
      Assert.assertNull(next);
    } finally {
      graph.drop();
    }
  }

  @Test
  public void testEnvelope() {

    OrientGraphNoTx graph = new OrientGraphNoTx("memory:functionsTest");
    try {
      ODatabaseDocumentTx db = graph.getRawGraph();

      List<ODocument> execute = db.command(new OCommandSQL("SELECT ST_AsText(ST_Envelope('LINESTRING(0 0, 1 3)'))")).execute();
      ODocument next = execute.iterator().next();
      Assert.assertEquals(next.field("ST_AsText"), "POLYGON ((0 0, 0 3, 1 3, 1 0, 0 0))");

    } finally {
      graph.drop();
    }
  }
}
