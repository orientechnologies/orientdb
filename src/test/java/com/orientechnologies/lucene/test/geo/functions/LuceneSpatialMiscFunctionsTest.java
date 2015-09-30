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
      Assert.assertNull(next.field("ST_AsBinary"));
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

  @Test
  public void testBuffer() {

    OrientGraphNoTx graph = new OrientGraphNoTx("memory:functionsTest");
    try {
      ODatabaseDocumentTx db = graph.getRawGraph();

      List<ODocument> execute = db.command(new OCommandSQL("SELECT ST_AsText(ST_Buffer(ST_GeomFromText('POINT(100 90)'),50));"))
          .execute();
      ODocument next = execute.iterator().next();
      Assert
          .assertEquals(
              next.field("ST_AsText"),
              "POLYGON ((150 90, 149.0392640201615 80.24548389919359, 146.19397662556435 70.86582838174552, 141.57348061512727 62.22148834901989, 135.35533905932738 54.64466094067263, 127.77851165098011 48.42651938487274, 119.1341716182545 43.80602337443566, 109.75451610080641 40.960735979838475, 100 40, 90.24548389919359 40.960735979838475, 80.86582838174552 43.80602337443566, 72.2214883490199 48.426519384872734, 64.64466094067262 54.64466094067262, 58.426519384872734 62.22148834901989, 53.80602337443566 70.86582838174553, 50.960735979838475 80.24548389919362, 50 90.00000000000004, 50.96073597983849 99.75451610080646, 53.80602337443568 109.13417161825454, 58.426519384872776 117.77851165098016, 64.64466094067268 125.35533905932743, 72.22148834901996 131.57348061512732, 80.8658283817456 136.19397662556437, 90.2454838991937 139.03926402016154, 100.00000000000013 140, 109.75451610080654 139.0392640201615, 119.13417161825463 136.1939766255643, 127.77851165098025 131.57348061512718, 135.3553390593275 125.35533905932726, 141.57348061512735 117.77851165097996, 146.1939766255644 109.13417161825431, 149.03926402016157 99.75451610080621, 150 90))");

      execute = db
          .command(new OCommandSQL("SELECT ST_AsText(ST_Buffer(ST_GeomFromText('POINT(100 90)'), 50, { quad_segs : 2 }));"))
          .execute();
      next = execute.iterator().next();

      Assert
          .assertEquals(
              next.field("ST_AsText"),
              "POLYGON ((150 90, 135.35533905932738 54.64466094067263, 100 40, 64.64466094067262 54.64466094067262, 50 90, 64.64466094067262 125.35533905932738, 99.99999999999999 140, 135.35533905932738 125.35533905932738, 150 90))");

    } finally {
      graph.drop();
    }
  }

  @Test
  public void testDistance() {

    OrientGraphNoTx graph = new OrientGraphNoTx("memory:functionsTest");
    try {
      ODatabaseDocumentTx db = graph.getRawGraph();

      List<ODocument> execute = db
          .command(
              new OCommandSQL(
                  "SELECT ST_Distance(ST_GeomFromText('POINT(-72.1235 42.3521)'),ST_GeomFromText('LINESTRING(-72.1260 42.45, -72.123 42.1546)'))"))
          .execute();
      ODocument next = execute.iterator().next();

      Assert.assertEquals(next.field("ST_Distance"), 0.00150567726382822);

      execute = db
          .command(
              new OCommandSQL(
                  "SELECT  ST_Distance( ST_GeomFromText('LINESTRING(13.45 52.47,13.46 52.48)'), ST_GeomFromText('LINESTRING(13.00 52.00,13.1 52.2)'))"))
          .execute();
      next = execute.iterator().next();

      Assert.assertEquals(next.field("ST_Distance"), 0.442040722106004);
    } finally {
      graph.drop();
    }

  }

  @Test
  public void testDisjoint() {

    OrientGraphNoTx graph = new OrientGraphNoTx("memory:functionsTest");
    try {
      ODatabaseDocumentTx db = graph.getRawGraph();

      List<ODocument> execute = db.command(new OCommandSQL("SELECT ST_Disjoint('POINT(0 0)', 'LINESTRING ( 2 0, 0 2 )');"))
          .execute();
      ODocument next = execute.iterator().next();

      Assert.assertEquals(next.field("ST_Disjoint"), true);

      execute = db.command(new OCommandSQL("SELECT ST_Disjoint('POINT(0 0)', 'LINESTRING ( 0 0, 0 2 )');")).execute();
      next = execute.iterator().next();

      Assert.assertEquals(next.field("ST_Disjoint"), false);
    } finally {
      graph.drop();
    }
  }

  @Test
  public void testWithin() {

    OrientGraphNoTx graph = new OrientGraphNoTx("memory:functionsTest");
    try {
      ODatabaseDocumentTx db = graph.getRawGraph();

      List<ODocument> execute = db
          .command(
              new OCommandSQL(
                  "select ST_Within(smallc,smallc) as smallinsmall,ST_Within(smallc, bigc) As smallinbig, ST_Within(bigc,smallc) As biginsmall from (SELECT ST_Buffer(ST_GeomFromText('POINT(50 50)'), 20) As smallc,ST_Buffer(ST_GeomFromText('POINT(50 50)'), 40) As bigc)"))
          .execute();
      ODocument next = execute.iterator().next();

      Assert.assertEquals(next.field("smallinsmall"), false);
      Assert.assertEquals(next.field("smallinbig"), true);
      Assert.assertEquals(next.field("biginsmall"), false);

    } finally {
      graph.drop();
    }
  }

}
