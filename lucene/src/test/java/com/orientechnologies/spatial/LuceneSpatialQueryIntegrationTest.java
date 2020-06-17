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

import com.orientechnologies.lucene.test.BaseLuceneTest;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Assert;
import org.junit.Test;

/** Created by Enrico Risa on 02/10/14. */
public class LuceneSpatialQueryIntegrationTest extends BaseLuceneTest {

  @Test
  public void testIssueGH9105() {

    db.command("create class Country extends V").close();
    db.command("create property Country.name STRING").close();
    db.command("create property Country.geometry EMBEDDED OMultiPolygon").close();
    db.command("create class POI extends V").close();
    db.command("create property POI.name STRING").close();
    db.command("create property POI.location EMBEDDED OPoint").close();
    db.command(
            "insert into POI(name, location) values(\"zeropoint\", St_GeomFromText(\"Point(0 0)\"))")
        .close();
    db.command(
            "insert into Country(name, geometry) values(\"zeroland\", St_GeomFromText(\"MultiPolygon(((1 1, 1 -1, -1 -1, -1 1, 1 1)))\"))")
        .close();
    db.command("CREATE INDEX POI.location ON POI(location) SPATIAL ENGINE LUCENE");
    db.command("CREATE INDEX Country.geometry ON Country(geometry) SPATIAL ENGINE LUCENE;");

    try (OResultSet resultSet =
        db.query(
            "select name from Country let locations = (select from Poi) where ST_Contains(geometry, $locations[0].location) = true")) {

      assertThat(resultSet.stream().count()).isEqualTo(1);
    }

    try (OResultSet resultSet =
        db.query(
            "select name from Country where ST_Contains(geometry, (select location from POI)) = true;")) {

      assertThat(resultSet.stream().count()).isEqualTo(1);
    }

    try (OResultSet resultSet =
        db.query(
            "select name from Country where ST_Contains(geometry, (select name,location from POI)) = true;")) {

      assertThat(resultSet.stream().count()).isEqualTo(0);
    }

    db.command(
            "insert into POI(name, location) values(\"zeropoint\", St_GeomFromText(\"Point(0 0)\"))")
        .close();

    try (OResultSet resultSet =
        db.query(
            "select name from Country where ST_Contains(geometry, (select location from POI)) = true;")) {

      Assert.fail("It should throw an exception");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof OCommandExecutionException);
    }

    db.command("delete vertex Poi").close();

    try (OResultSet resultSet =
        db.query(
            "select name from Country where ST_Contains(geometry, (select location from POI)) = true;")) {

      assertThat(resultSet.stream().count()).isEqualTo(0);
    }
  }
}
