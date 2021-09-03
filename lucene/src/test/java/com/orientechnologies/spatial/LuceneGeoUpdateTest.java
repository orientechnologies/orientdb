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

import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.junit.Assert;
import org.junit.Test;

/** Created by Enrico Risa on 26/09/15. */
public class LuceneGeoUpdateTest extends BaseSpatialLuceneTest {

  @Test
  public void testUpdate() {

    db.command(new OCommandSQL("create class City extends V")).execute();

    db.command(new OCommandSQL("create property City.location embedded OPoint")).execute();

    db.command(
            new OCommandSQL("CREATE INDEX City.location ON City(location) SPATIAL ENGINE LUCENE"))
        .execute();
    db.command(
            new OCommandSQL(
                "insert into City set name = 'TestInsert' , location = ST_GeomFromText('POINT(-160.2075374 21.9029803)')"))
        .execute();

    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "City.location");

    db.command(
            new OCommandSQL(
                "update City set name = 'TestInsert' , location = ST_GeomFromText('POINT(12.5 41.9)')"))
        .execute();

    Assert.assertEquals(1, index.getInternal().size());
  }
}
