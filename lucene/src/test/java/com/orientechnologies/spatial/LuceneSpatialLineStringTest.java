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

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Created by Enrico Risa on 07/08/15. */
public class LuceneSpatialLineStringTest extends BaseSpatialLuceneTest {

  public static String LINEWKT =
      "LINESTRING(-149.8871332 61.1484656,-149.8871655 61.1489556,-149.8871569 61.15043,-149.8870366 61.1517722)";

  @Before
  public void initMore() {
    db.set(ODatabase.ATTRIBUTES.CUSTOM, "strictSql=false");
    OSchema schema = db.getMetadata().getSchema();
    OClass v = schema.getClass("V");
    OClass oClass = schema.createClass("Place");
    oClass.setSuperClass(v);
    oClass.createProperty("location", OType.EMBEDDED, schema.getClass("OLineString"));
    oClass.createProperty("name", OType.STRING);

    db.command(
            new OCommandSQL("CREATE INDEX Place.location ON Place(location) SPATIAL ENGINE LUCENE"))
        .execute();

    ODocument linestring1 = new ODocument("Place");
    linestring1.field("name", "LineString1");
    linestring1.field(
        "location",
        createLineString(
            new ArrayList<List<Double>>() {
              {
                add(Arrays.asList(0d, 0d));
                add(Arrays.asList(3d, 3d));
              }
            }));

    ODocument linestring2 = new ODocument("Place");
    linestring2.field("name", "LineString2");
    linestring2.field(
        "location",
        createLineString(
            new ArrayList<List<Double>>() {
              {
                add(Arrays.asList(0d, 1d));
                add(Arrays.asList(0d, 5d));
              }
            }));
    db.save(linestring1);
    db.save(linestring2);

    db.command(
            new OCommandSQL(
                "insert into Place set name = 'LineString3' , location = ST_GeomFromText('"
                    + LINEWKT
                    + "')"))
        .execute();
  }

  //  @After
  //  public void deInit() {
  //    deInitDB();
  //  }

  public ODocument createLineString(List<List<Double>> coordinates) {
    ODocument location = new ODocument("OLineString");
    location.field("coordinates", coordinates);
    return location;
  }

  @Test
  public void testLineStringWithoutIndex() throws IOException {
    db.command(new OCommandSQL("drop index Place.location")).execute();
    queryLineString();
  }

  protected void queryLineString() {
    String query =
        "select * from Place where location && { 'shape' : { 'type' : 'OLineString' , 'coordinates' : [[1,2],[4,6]]} } ";
    List<ODocument> docs = db.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(docs.size(), 1);

    query = "select * from Place where location && 'LINESTRING(1 2, 4 6)' ";
    docs = db.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(docs.size(), 1);

    query = "select * from Place where location && ST_GeomFromText('LINESTRING(1 2, 4 6)') ";
    docs = db.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(docs.size(), 1);

    query =
        "select * from Place where location && 'POLYGON((-150.205078125 61.40723633876356,-149.2657470703125 61.40723633876356,-149.2657470703125 61.05562700886678,-150.205078125 61.05562700886678,-150.205078125 61.40723633876356))' ";
    docs = db.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(docs.size(), 1);
  }

  @Test
  public void testIndexingLineString() throws IOException {

    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "Place.location");

    Assert.assertEquals(3, index.getInternal().size());
    queryLineString();
  }
}
