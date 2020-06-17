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
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.spatial.BaseSpatialLuceneTest;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.spatial4j.distance.DistanceUtils;

/** Created by Enrico Risa on 28/09/15. */
public class LuceneSpatialDistanceSphereTest extends BaseSpatialLuceneTest {

  @Test
  public void testDistanceSphereNoIndex() {

    List<ODocument> execute =
        db.command(
                new OCommandSQL(
                    "select ST_Distance(ST_GEOMFROMTEXT('POINT(12.4662748 41.8914114)'),ST_GEOMFROMTEXT('POINT(12.4664632 41.8904382)')) as distanceDeg, \n"
                        + "ST_Distance_Sphere(ST_GEOMFROMTEXT('POINT(12.4662748 41.8914114)'),ST_GEOMFROMTEXT('POINT(12.4664632 41.8904382)')) as distanceMeter"))
            .execute();

    Assert.assertEquals(1, execute.size());
    ODocument next = execute.iterator().next();

    Double distanceDeg = next.field("distanceDeg");
    Double distanceMeter = next.field("distanceMeter");
    Assert.assertNotNull(distanceDeg);
    Assert.assertNotNull(distanceMeter);

    Assert.assertEquals(109, distanceMeter.intValue());

    Double d =
        DistanceUtils.degrees2Dist(distanceDeg, DistanceUtils.EARTH_EQUATORIAL_RADIUS_KM) * 1000;
    Assert.assertNotEquals(d, distanceMeter);
  }

  // TODO
  // Need more test with index
  @Test
  public void testWithinIndex() {

    db.command(new OCommandSQL("create class Place extends v")).execute();
    db.command(new OCommandSQL("create property Place.location EMBEDDED OPoint")).execute();

    db.command(
            new OCommandSQL(
                "insert into Place set name =  'Dar Poeta',location = ST_GeomFromText('POINT(12.4684635 41.8914114)')"))
        .execute();
    db.command(
            new OCommandSQL(
                "insert into Place set name  = 'Antilia Pub',location = ST_GeomFromText('POINT(12.4686519 41.890438)')"))
        .execute();

    db.command(
            new OCommandSQL(
                "insert into Place set name = 'Museo Di Roma in Trastevere',location = ST_GeomFromText('POINT(12.4689762 41.8898916)')"))
        .execute();

    db.command(new OCommandSQL("create index Place.l on Place (location) SPATIAL engine lucene"))
        .execute();
    List<ODocument> execute =
        db.command(
                new OCommandSQL(
                    "SELECT from Place where ST_Distance_Sphere(location, ST_GeomFromText('POINT(12.468933 41.890303)')) < 50"))
            .execute();

    Assert.assertEquals(2, execute.size());

    execute =
        db.command(
                new OCommandSQL(
                    "SELECT from Place where ST_Distance_Sphere(location, ST_GeomFromText('POINT(12.468933 41.890303)')) > 50"))
            .execute();

    Assert.assertEquals(1, execute.size());
  }

  // Need more test with index
  @Test
  public void testDistanceProjection() {

    db.command(new OCommandSQL("create class Restaurant extends v")).execute();
    db.command(new OCommandSQL("create property Restaurant.location EMBEDDED OPoint")).execute();

    db.command(
            new OCommandSQL(
                "INSERT INTO  Restaurant SET name = 'London', location = St_GeomFromText(\"POINT (-0.1277583 51.5073509)\")"))
        .execute();
    db.command(
            new OCommandSQL(
                "INSERT INTO  Restaurant SET name = 'Trafalgar', location = St_GeomFromText(\"POINT (-0.1280688 51.5080388)\")"))
        .execute();

    db.command(
            new OCommandSQL(
                "INSERT INTO  Restaurant SET name = 'Lambeth North Station', location = St_GeomFromText(\"POINT (-0.1120681 51.4989103)\")"))
        .execute();

    db.command(
            new OCommandSQL(
                "INSERT INTO  Restaurant SET name = 'Montreal', location = St_GeomFromText(\"POINT (-73.567256 45.5016889)\")"))
        .execute();

    db.command(
            new OCommandSQL("CREATE INDEX bla ON Restaurant (location) SPATIAL ENGINE LUCENE;\n"))
        .execute();
    List<OResult> execute =
        db
            .query(
                "SELECT  ST_Distance_Sphere(location, St_GeomFromText(\"POINT (-0.1277583 51.5073509)\")) as dist from Restaurant;")
            .stream()
            .collect(Collectors.toList());

    Assert.assertEquals(4, execute.size());
  }

  @Test
  public void testNullObject() {
    List<ODocument> execute =
        db.command(
                new OCommandSQL(
                    "select ST_Distance({ locationCoordinates: null },ST_GEOMFROMTEXT('POINT(12.4664632 41.8904382)')) as distanceMeter"))
            .execute();

    Assert.assertEquals(1, execute.size());
    ODocument next = execute.iterator().next();
    Assert.assertTrue(next.isEmpty());
  }

  @Test
  public void testSphereNullObject() {
    List<ODocument> execute =
        db.command(
                new OCommandSQL(
                    "select ST_Distance_Sphere({ locationCoordinates: null },ST_GEOMFROMTEXT('POINT(12.4664632 41.8904382)')) as distanceMeter"))
            .execute();

    Assert.assertEquals(1, execute.size());
    ODocument next = execute.iterator().next();
    Assert.assertTrue(next.isEmpty());
  }
}
