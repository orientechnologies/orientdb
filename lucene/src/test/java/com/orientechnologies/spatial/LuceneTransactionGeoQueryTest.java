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

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/** Created by Enrico Risa on 05/10/15. */
public class LuceneTransactionGeoQueryTest {

  private static String PWKT = "POINT(-160.2075374 21.9029803)";

  @Test
  public void testPointTransactionRollBack() {

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:txPoint");

    try {
      db.create();

      OSchema schema = db.getMetadata().getSchema();
      OClass v = schema.getClass("V");
      OClass oClass = schema.createClass("City");
      oClass.setSuperClass(v);
      oClass.createProperty("location", OType.EMBEDDED, schema.getClass("OPoint"));
      oClass.createProperty("name", OType.STRING);

      db.command(
              new OCommandSQL("CREATE INDEX City.location ON City(location) SPATIAL ENGINE LUCENE"))
          .execute();

      OIndex idx = db.getMetadata().getIndexManagerInternal().getIndex(db, "City.location");
      ODocument rome = newCity("Rome", 12.5, 41.9);
      ODocument london = newCity("London", -0.1275, 51.507222);

      db.begin();

      db.command(
              new OCommandSQL(
                  "insert into City set name = 'TestInsert' , location = ST_GeomFromText('"
                      + PWKT
                      + "')"))
          .execute();
      db.save(rome);
      db.save(london);
      String query =
          "select * from City where location && 'LINESTRING(-160.06393432617188 21.996535232496047,-160.1099395751953 21.94304553343818,-160.169677734375 21.89399562866819,-160.21087646484375 21.844928843026818,-160.21018981933594 21.787556698550834)' ";
      List<ODocument> docs = db.query(new OSQLSynchQuery<ODocument>(query));
      Assert.assertEquals(1, docs.size());
      Assert.assertEquals(3, idx.getInternal().size());
      db.rollback();

      query =
          "select * from City where location && 'LINESTRING(-160.06393432617188 21.996535232496047,-160.1099395751953 21.94304553343818,-160.169677734375 21.89399562866819,-160.21087646484375 21.844928843026818,-160.21018981933594 21.787556698550834)' ";
      docs = db.query(new OSQLSynchQuery<ODocument>(query));
      Assert.assertEquals(0, docs.size());
      Assert.assertEquals(0, idx.getInternal().size());
    } finally {
      db.drop();
    }
  }

  @Test
  public void testPointTransactionUpdate() {

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:txPoint");

    try {
      db.create();

      OSchema schema = db.getMetadata().getSchema();
      OClass v = schema.getClass("V");
      OClass oClass = schema.createClass("City");
      oClass.setSuperClass(v);
      oClass.createProperty("location", OType.EMBEDDED, schema.getClass("OPoint"));
      oClass.createProperty("name", OType.STRING);

      db.command(
              new OCommandSQL("CREATE INDEX City.location ON City(location) SPATIAL ENGINE LUCENE"))
          .execute();

      OIndex idx = db.getMetadata().getIndexManagerInternal().getIndex(db, "City.location");
      ODocument rome = newCity("Rome", 12.5, 41.9);

      db.begin();

      db.save(rome);

      db.commit();

      String query =
          "select * from City where location && 'LINESTRING(-160.06393432617188 21.996535232496047,-160.1099395751953 21.94304553343818,-160.169677734375 21.89399562866819,-160.21087646484375 21.844928843026818,-160.21018981933594 21.787556698550834)' ";
      List<ODocument> docs = db.query(new OSQLSynchQuery<ODocument>(query));
      Assert.assertEquals(0, docs.size());
      Assert.assertEquals(1, idx.getInternal().size());

      db.begin();

      db.command(new OCommandSQL("update City set location = ST_GeomFromText('" + PWKT + "')"))
          .execute();

      query =
          "select * from City where location && 'LINESTRING(-160.06393432617188 21.996535232496047,-160.1099395751953 21.94304553343818,-160.169677734375 21.89399562866819,-160.21087646484375 21.844928843026818,-160.21018981933594 21.787556698550834)' ";
      docs = db.query(new OSQLSynchQuery<ODocument>(query));
      Assert.assertEquals(1, docs.size());
      Assert.assertEquals(1, idx.getInternal().size());

      db.commit();

      query =
          "select * from City where location && 'LINESTRING(-160.06393432617188 21.996535232496047,-160.1099395751953 21.94304553343818,-160.169677734375 21.89399562866819,-160.21087646484375 21.844928843026818,-160.21018981933594 21.787556698550834)' ";
      docs = db.query(new OSQLSynchQuery<ODocument>(query));
      Assert.assertEquals(1, docs.size());
      Assert.assertEquals(1, idx.getInternal().size());

    } finally {
      db.drop();
    }
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
