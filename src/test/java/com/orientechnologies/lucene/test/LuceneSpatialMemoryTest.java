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

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Created by Enrico Risa on 07/10/15.
 */
@Test(groups = "embedded")
public class LuceneSpatialMemoryTest {

  @Test
  public void boundingBoxTest() {

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:test");

    db.create();

    try {

      OClass point = db.getMetadata().getSchema().createClass("Point");
      point.createProperty("latitude", OType.DOUBLE);
      point.createProperty("longitude", OType.DOUBLE);

      db.command(new OCommandSQL("CREATE INDEX Point.ll ON Point(latitude,longitude) SPATIAL ENGINE LUCENE")).execute();

      ODocument document = new ODocument("Point");

      document.field("latitude", 42.2814837);
      document.field("longitude", -83.7605452);

      db.save(document);

      List<?> query = db
          .query(new OSQLSynchQuery<ODocument>(
              "SELECT FROM Point WHERE [latitude, longitude] WITHIN [[42.26531323615103,-83.71986351411135],[42.29239784478525,-83.7662120858887]]"));

      Assert.assertEquals(query.size(), 1);
    } finally {
      db.drop();
    }
  }

}