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
package com.orientechnologies.spatial;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Created by Enrico Risa on 07/08/15. */
public class LuceneSpatialPointTest extends BaseSpatialLuceneTest {

  private static String PWKT = "POINT(-160.2075374 21.9029803)";

  @Before
  public void init() {

    OSchema schema = db.getMetadata().getSchema();
    OClass v = schema.getClass("V");
    OClass oClass = schema.createClass("City");
    oClass.setSuperClass(v);
    oClass.createProperty("location", OType.EMBEDDED, schema.getClass("OPoint"));
    oClass.createProperty("name", OType.STRING);

    OClass place = schema.createClass("Place");
    place.setSuperClass(v);
    place.createProperty("latitude", OType.DOUBLE);
    place.createProperty("longitude", OType.DOUBLE);
    place.createProperty("name", OType.STRING);

    db.command(
            new OCommandSQL("CREATE INDEX City.location ON City(location) SPATIAL ENGINE LUCENE"))
        .execute();

    db.command(
            new OCommandSQL(
                "CREATE INDEX Place.l_lon ON Place(latitude,longitude) SPATIAL ENGINE LUCENE"))
        .execute();

    ODocument rome = newCity("Rome", 12.5, 41.9);
    ODocument london = newCity("London", -0.1275, 51.507222);

    ODocument rome1 = new ODocument("Place");
    rome1.field("name", "Rome");
    rome1.field("latitude", 41.9);
    rome1.field("longitude", 12.5);
    db.save(rome1);
    db.save(rome);
    db.save(london);

    db.command(
            new OCommandSQL(
                "insert into City set name = 'TestInsert' , location = ST_GeomFromText('"
                    + PWKT
                    + "')"))
        .execute();
  }

  @Test
  public void testPointWithoutIndex() {

    db.command(new OCommandSQL("Drop INDEX City.location")).execute();
    queryPoint();
  }

  @Test
  public void testIndexingPoint() {

    queryPoint();
  }

  protected void queryPoint() {
    // TODO remove = true when parser will support index function without expression
    String query =
        "select * from City where  ST_WITHIN(location,{ 'shape' : { 'type' : 'ORectangle' , 'coordinates' : [12.314015,41.8262816,12.6605063,41.963125]} })"
            + " = true";
    List<ODocument> docs = db.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(1, docs.size());

    query =
        "select * from City where  ST_WITHIN(location,'POLYGON ((12.314015 41.8262816, 12.314015 41.963125, 12.6605063 41.963125, 12.6605063 41.8262816, 12.314015 41.8262816))')"
            + " = true";
    docs = db.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(1, docs.size());

    query =
        "select * from City where  ST_WITHIN(location,ST_GeomFromText('POLYGON ((12.314015 41.8262816, 12.314015 41.963125, 12.6605063 41.963125, 12.6605063 41.8262816, 12.314015 41.8262816))'))"
            + " = true";
    docs = db.query(new OSQLSynchQuery<ODocument>(query));
    Assert.assertEquals(1, docs.size());

    query =
        "select * from City where location && 'LINESTRING(-160.06393432617188 21.996535232496047,-160.1099395751953 21.94304553343818,-160.169677734375 21.89399562866819,-160.21087646484375 21.844928843026818,-160.21018981933594 21.787556698550834)' ";
    docs = db.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(1, docs.size());
  }

  @Test
  public void testOldNearQuery() {

    queryOldNear();
  }

  protected void queryOldNear() {
    String query =
        "select *,$distance from Place where [latitude,longitude,$spatial] NEAR [41.893056,12.482778,{\"maxDistance\": 2}]";
    List<ODocument> docs = db.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(1, docs.size());

    //    Assert.assertEquals(1.6229442709302933, docs.get(0).field("$distance"));

    assertThat(docs.get(0).<Double>field("$distance")).isEqualTo(1.6229442709302933);
  }

  @Test
  public void testOldNearQueryWithoutIndex() {

    db.command(new OCommandSQL("Drop INDEX Place.l_lon")).execute();
    queryOldNear();
  }

  protected ODocument newCity(String name, final Double longitude, final Double latitude) {

    ODocument location = new ODocument("OPoint");
    location.field(
        "coordinates",
        new ArrayList<Double>() {
          {
            add(longitude);
            add(latitude);
          }
        });

    ODocument city = new ODocument("City");
    city.field("name", name);
    city.field("location", location);
    return city;
  }
}
