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

import com.orientechnologies.lucene.test.BaseSpatialLuceneTest;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import junit.framework.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Enrico Risa on 07/08/15.
 */

@Test
public class LuceneSpatialPointTest extends BaseSpatialLuceneTest {
  @Override
  protected String getDatabaseName() {
    return "spatialPointTest";
  }

  @BeforeClass
  public void init() {
    initDB();

    databaseDocumentTx.set(ODatabase.ATTRIBUTES.CUSTOM, "strictSql=false");
    OSchema schema = databaseDocumentTx.getMetadata().getSchema();
    OClass v = schema.getClass("V");
    OClass oClass = schema.createClass("City");
    oClass.setSuperClass(v);
    oClass.createProperty("location", OType.EMBEDDED, schema.getClass("Point"));
    oClass.createProperty("name", OType.STRING);

    OClass place = schema.createClass("Place");
    place.setSuperClass(v);
    place.createProperty("latitude", OType.DOUBLE);
    place.createProperty("longitude", OType.DOUBLE);
    place.createProperty("name", OType.STRING);

    databaseDocumentTx.command(new OCommandSQL("CREATE INDEX City.location ON City(location) SPATIAL ENGINE LUCENE")).execute();

    databaseDocumentTx.command(new OCommandSQL("CREATE INDEX Place.l_lon ON Place(latitude,longitude) SPATIAL ENGINE LUCENE"))
        .execute();

    ODocument rome = newCity("Rome", 12.5, 41.9);
    ODocument london = newCity("London", -0.1275, 51.507222);

    databaseDocumentTx.save(rome);
    databaseDocumentTx.save(london);

  }

  @Test
  public void testPointWithoutIndex() {

    databaseDocumentTx.command(new OCommandSQL("Drop INDEX City.location")).execute();
    queryPoint();
  }

  public void testIndexingPoint() {

    queryPoint();
  }

  protected void queryPoint() {
    String query = "select * from City where location ST_WITHIN { 'shape' : { 'type' : 'Rectangle' , 'coordinates' : [12.314015,41.8262816,12.6605063,41.963125]} } ";
    List<ODocument> docs = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(1, docs.size());

    query = "select *,$distance from City where location ST_NEAR { 'shape' : { 'type' : 'Point' , 'coordinates' : [12.482778,41.893056] } , 'maxDistance' : 2  } ";
    docs = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(1, docs.size());

    Assert.assertEquals(1.6229442709302933, docs.get(0).field("$distance"));
  }

  public void testOldNearQuery() {

    ODocument rome = new ODocument("Place");
    rome.field("name", "Rome");
    rome.field("latitude", 41.9);
    rome.field("longitude", 12.5);
    databaseDocumentTx.save(rome);
    String query = "select *,$distance from Place where [latitude,longitude,$spatial] NEAR [41.893056,12.482778,{\"maxDistance\": 2}]";
    List<ODocument> docs = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(1, docs.size());

    Assert.assertEquals(1.6229442709302933, docs.get(0).field("$distance"));
  }

  protected ODocument newCity(String name, final Double longitude, final Double latitude) {

    ODocument location = new ODocument("Point");
    location.field("coordinates", new ArrayList<Double>() {
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

  @AfterClass
  public void deInit() {
    deInitDB();
  }
}
