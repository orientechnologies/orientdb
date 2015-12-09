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

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Created by Enrico Risa on 26/09/15.
 */
public class LuceneGeoUpdateTest {

  @Test
  public void testUpdate() {

    OrientGraphNoTx graph = new OrientGraphNoTx("memory:doubleLucene");
    try {
      ODatabaseDocumentTx db = graph.getRawGraph();

      db.command(new OCommandSQL("create class City extends V")).execute();

      db.command(new OCommandSQL("create property City.location embedded OPoint")).execute();

      db.command(new OCommandSQL("CREATE INDEX City.location ON City(location) SPATIAL ENGINE LUCENE")).execute();
      db.command(
          new OCommandSQL("insert into City set name = 'Test' , location = ST_GeomFromText('POINT(-160.2075374 21.9029803)')"))
          .execute();

      OIndex<?> index = db.getMetadata().getIndexManager().getIndex("City.location");

      db.command(new OCommandSQL("update City set name = 'Test' , location = ST_GeomFromText('POINT(12.5 41.9)')")).execute();

      Assert.assertEquals(index.getSize(), 1);

    } finally {
      graph.drop();
    }

  }

}
