/**
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * <p>For more information: http://www.orientdb.com
 */
package com.orientechnologies.spatial.functions;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.spatial.BaseSpatialLuceneTest;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/** Created by Enrico Risa on 28/09/15. */
public class LuceneSpatialContainsTest extends BaseSpatialLuceneTest {

  @Test
  public void testContainsNoIndex() {

    List<ODocument> execute =
        db.command(
                new OCommandSQL(
                    "select ST_Contains(smallc,smallc) as smallinsmall,ST_Contains(smallc, bigc) As smallinbig, ST_Contains(bigc,smallc) As biginsmall from (SELECT ST_Buffer(ST_GeomFromText('POINT(50 50)'), 20) As smallc,ST_Buffer(ST_GeomFromText('POINT(50 50)'), 40) As bigc)"))
            .execute();
    ODocument next = execute.iterator().next();

    Assert.assertTrue(next.field("smallinsmall"));
    Assert.assertFalse(next.field("smallinbig"));
    Assert.assertTrue(next.field("biginsmall"));
  }

  @Test
  public void testContainsIndex() {

    db.command(new OCommandSQL("create class Polygon extends v")).execute();
    db.command(new OCommandSQL("create property Polygon.geometry EMBEDDED OPolygon")).execute();

    db.command(
            new OCommandSQL(
                "insert into Polygon set geometry = ST_Buffer(ST_GeomFromText('POINT(50 50)'), 20)"))
        .execute();
    db.command(
            new OCommandSQL(
                "insert into Polygon set geometry = ST_Buffer(ST_GeomFromText('POINT(50 50)'), 40)"))
        .execute();

    db.command(
            new OCommandSQL("create index Polygon.g on Polygon (geometry) SPATIAL engine lucene"))
        .execute();
    List<ODocument> execute =
        db.command(
                new OCommandSQL(
                    "SELECT from Polygon where ST_Contains(geometry, 'POINT(50 50)') = true"))
            .execute();

    Assert.assertEquals(2, execute.size());

    execute =
        db.command(
                new OCommandSQL(
                    "SELECT from Polygon where ST_Contains(geometry, ST_Buffer(ST_GeomFromText('POINT(50 50)'), 30)) = true"))
            .execute();

    Assert.assertEquals(1, execute.size());
  }

  @Test
  public void testContainsIndex_GeometryCollection() {

    db.command(new OCommandSQL("create class TestInsert extends v")).execute();
    db.command(new OCommandSQL("create property TestInsert.geometry EMBEDDED OGeometryCollection"))
        .execute();

    db.command(
            new OCommandSQL(
                "insert into TestInsert set geometry = {'@type':'d','@class':'OGeometryCollection','geometries':[{'@type':'d','@class':'OPolygon','coordinates':[[[0,0],[10,0],[10,10],[0,10],[0,0]]]}]}"))
        .execute();
    db.command(
            new OCommandSQL(
                "insert into TestInsert set geometry = {'@type':'d','@class':'OGeometryCollection','geometries':[{'@type':'d','@class':'OPolygon','coordinates':[[[11,11],[21,11],[21,21],[11,21],[11,11]]]}]}"))
        .execute();

    db.command(
            new OCommandSQL(
                "create index TestInsert.geometry on TestInsert (geometry) SPATIAL engine lucene"))
        .execute();

    String testGeometry =
        "{'@type':'d','@class':'OGeometryCollection','geometries':[{'@type':'d','@class':'OPolygon','coordinates':[[[1,1],[2,1],[2,2],[1,2],[1,1]]]}]}";
    List<ODocument> execute =
        db.command(
                new OCommandSQL(
                    "SELECT from TestInsert where ST_Contains(geometry, "
                        + testGeometry
                        + ") = true"))
            .execute();

    Assert.assertEquals(1, execute.size());
  }
}
