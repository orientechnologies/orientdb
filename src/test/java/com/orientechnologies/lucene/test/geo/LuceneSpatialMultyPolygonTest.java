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
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

/**
 * Created by Enrico Risa on 07/08/15.
 */

@Test(groups = "embedded")
public class LuceneSpatialMultyPolygonTest extends BaseSpatialLuceneTest {
  @Override
  protected String getDatabaseName() {
    return "spatialMultiPolygonTest";
  }

  @BeforeClass
  public void init() {
    initDB();

    databaseDocumentTx.set(ODatabase.ATTRIBUTES.CUSTOM, "strictSql=false");
    OSchema schema = databaseDocumentTx.getMetadata().getSchema();
    OClass v = schema.getClass("V");
    OClass oClass = schema.createClass("Place");
    oClass.setSuperClass(v);
    oClass.createProperty("location", OType.EMBEDDED, schema.getClass("MultiPolygon"));
    oClass.createProperty("name", OType.STRING);

    databaseDocumentTx.command(new OCommandSQL("CREATE INDEX Place.location ON Place(location) SPATIAL ENGINE LUCENE")).execute();

  }

  @Test(enabled = false)
  public void testMultiPolygonWithoutIndex() {
    databaseDocumentTx.command(new OCommandSQL("DROP INDEX Place.location")).execute();
    queryMultiPolygon();
  }

  @Test(enabled = false)
  public void testIndexingMultiPolygon() throws IOException {

    ODocument location = loadMultiPolygon();

    ODocument germany = new ODocument("Place");
    germany.field("name", "Italy");
    germany.field("location", location);
    databaseDocumentTx.save(germany);

    OIndex<?> index = databaseDocumentTx.getMetadata().getIndexManager().getIndex("Place.location");

    Assert.assertEquals(index.getSize(), 1);

    queryMultiPolygon();

  }

  // DISABLED
  protected void queryMultiPolygon() {
    // Should not contain Berlin
    String query = "select * from Place where location ST_CONTAINS { 'shape' : { 'type' : 'Point' , 'coordinates' : [13.383333,52.516667]} } ";
    List<ODocument> docs = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(docs.size(), 0);

    // Should not contain Berlin BBox
    query = "select * from Place where location ST_CONTAINS { 'shape' : { 'type' : 'Rectangle' , 'coordinates' : [13.0884,52.33812,13.76134,52.675499]} } ";
    docs = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(docs.size(), 0);

    // Should contain contain Rome
    query = "select * from Place where location ST_CONTAINS { 'shape' : { 'type' : 'Point' , 'coordinates' : [12.5,41.9]} } ";
    docs = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(docs.size(), 1);

    // Should contain Rome BBox
    query = "select * from Place where location ST_CONTAINS { 'shape' : { 'type' : 'Rectangle' , 'coordinates' : [12.37528,41.802872,12.62256,41.991791]} } ";
    docs = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(docs.size(), 1);

    // Should contain contain Catania

    query = "select * from Place where location ST_CONTAINS { 'shape' : { 'type' : 'Point' , 'coordinates' : [15.0777,37.507999]} } ";
    docs = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(docs.size(), 1);

    // Should contain Catania BBox

    query = "select * from Place where location ST_CONTAINS { 'shape' : { 'type' : 'Rectangle' , 'coordinates' : [15.04145,37.470379,15.11752,37.532421]} } ";
    docs = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(docs.size(), 1);
  }

  @AfterClass
  public void deInit() {
    deInitDB();
  }
}
