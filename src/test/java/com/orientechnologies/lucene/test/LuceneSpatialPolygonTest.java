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

package com.orientechnologies.lucene.test;

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
import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * Created by Enrico Risa on 07/08/15.
 */

@Test
public class LuceneSpatialPolygonTest extends BaseSpatialLuceneTest {
  @Override
  protected String getDatabaseName() {
    return "spatialPolygonTest";
  }

  @BeforeClass
  public void init() {
    initDB();

    databaseDocumentTx.set(ODatabase.ATTRIBUTES.CUSTOM, "strictSql=false");
    OSchema schema = databaseDocumentTx.getMetadata().getSchema();
    OClass v = schema.getClass("V");
    OClass oClass = schema.createClass("Place");
    oClass.setSuperClass(v);
    oClass.createProperty("location", OType.EMBEDDED, schema.getClass("Polygon"));
    oClass.createProperty("name", OType.STRING);

    databaseDocumentTx.command(new OCommandSQL("CREATE INDEX Place.location ON Place(location) SPATIAL ENGINE LUCENE")).execute();

  }

  @Test
  public void testIndexingPolygon() throws IOException {

    InputStream systemResourceAsStream = ClassLoader.getSystemResourceAsStream("germany.json");

    ODocument doc = new ODocument().fromJSON(systemResourceAsStream);

    Map geometry = doc.field("geometry");

    String type = (String) geometry.get("type");
    ODocument location = new ODocument(type);
    location.field("coordinates", geometry.get("coordinates"));
    ODocument germany = new ODocument("Place");
    germany.field("name", "Germany");
    germany.field("location", location);
    databaseDocumentTx.save(germany);

    OIndex<?> index = databaseDocumentTx.getMetadata().getIndexManager().getIndex("Place.location");

    Assert.assertEquals(index.getSize(), 1);

    // Should contain Berlin
    String query = "select * from Place where location STContains { 'shape' : { 'type' : 'Point' , 'coordinates' : [13.383333,52.516667]} } ";
    List<ODocument> docs = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(docs.size(), 1);

    // Should contain Berlin BBox
    query = "select * from Place where location STContains { 'shape' : { 'type' : 'Rectangle' , 'coordinates' : [13.0884,52.33812,13.76134,52.675499]} } ";
    docs = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(docs.size(), 1);

    // Should not contain Rome
    query = "select * from Place where location STContains { 'shape' : { 'type' : 'Point' , 'coordinates' : [12.5,41.9]} } ";
    docs = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(docs.size(), 0);

    // Should not contain Rome BBox
    query = "select * from Place where location STContains { 'shape' : { 'type' : 'Rectangle' , 'coordinates' : [12.37528,41.802872,12.62256,41.991791]} } ";
    docs = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(docs.size(), 0);

  }

  @AfterClass
  public void deInit() {
    deInitDB();
  }
}
