package com.orientechnologies.spatial;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class GeometryCollectionTest extends BaseSpatialLuceneTest {

  @Test
  public void testDeleteVerticesWithGeometryCollection() {
    db.command(new OCommandSQL("CREATE CLASS TestInsert extends V")).execute();
    db.command(new OCommandSQL("CREATE PROPERTY TestInsert.name STRING")).execute();
    db.command(new OCommandSQL("CREATE PROPERTY TestInsert.geometry EMBEDDED OGeometryCollection"))
        .execute();

    db.command(
            new OCommandSQL(
                "CREATE INDEX TestInsert.geometry ON TestInsert(geometry) SPATIAL ENGINE LUCENE"))
        .execute();

    db.command(
            new OCommandSQL(
                "insert into TestInsert content {'name': 'loc1', 'geometry': {'@type':'d','@class':'OGeometryCollection','geometries':[{'@type':'d','@class':'OPolygon','coordinates':[[[0,0],[0,10],[10,10],[10,0],[0,0]]]}]}}"))
        .execute();
    db.command(
            new OCommandSQL(
                "insert into TestInsert content {'name': 'loc2', 'geometry': {'@type':'d','@class':'OGeometryCollection','geometries':[{'@type':'d','@class':'OPolygon','coordinates':[[[0,0],[0,20],[20,20],[20,0],[0,0]]]}]}}"))
        .execute();

    List<ODocument> qResult =
        db.command(
                new OCommandSQL(
                    "select * from TestInsert where ST_WITHIN(geometry,'POLYGON ((0 0, 15 0, 15 15, 0 15, 0 0))') = true"))
            .execute();
    Assert.assertEquals(1, qResult.size());

    db.command(new OCommandSQL("DELETE VERTEX TestInsert")).execute();

    List<ODocument> qResult2 = db.command(new OCommandSQL("select * from TestInsert")).execute();
    Assert.assertEquals(0, qResult2.size());
  }
}
