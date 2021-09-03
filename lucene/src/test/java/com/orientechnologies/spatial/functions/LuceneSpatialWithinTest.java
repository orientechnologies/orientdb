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
package com.orientechnologies.spatial.functions;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.spatial.BaseSpatialLuceneTest;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/** Created by Enrico Risa on 28/09/15. */
public class LuceneSpatialWithinTest extends BaseSpatialLuceneTest {

  @Test
  public void testWithinNoIndex() {

    List<ODocument> execute =
        db.command(
                new OCommandSQL(
                    "select ST_Within(smallc,smallc) as smallinsmall,ST_Within(smallc, bigc) As smallinbig, ST_Within(bigc,smallc) As biginsmall "
                        + "from (SELECT ST_Buffer(ST_GeomFromText('POINT(50 50)'), 20) As smallc,ST_Buffer(ST_GeomFromText('POINT(50 50)'), 40) As bigc)"))
            .execute();
    ODocument next = execute.iterator().next();

    Assert.assertEquals(next.field("smallinsmall"), true);
    Assert.assertEquals(next.field("smallinbig"), true);
    Assert.assertEquals(next.field("biginsmall"), false);
  }

  @Test
  public void testWithinIndex() {

    db.command(new OCommandSQL("create class Polygon extends v")).execute();
    db.command(new OCommandSQL("create property Polygon.geometry EMBEDDED OPolygon")).execute();

    db.command(
            new OCommandSQL(
                "insert into Polygon set geometry = ST_Buffer(ST_GeomFromText('POINT(50 50)'), 20)"))
        .execute();
    db.command(
            new OCommandSQL(
                "insert into Polygon set geometry = ST_Buffer(ST_GeomFromText('POINT(50 50)'), 40)"))
        .execute();

    db.command(
            new OCommandSQL("create index Polygon.g on Polygon (geometry) SPATIAL engine lucene"))
        .execute();
    List<ODocument> execute =
        db.command(
                new OCommandSQL(
                    "SELECT from Polygon where ST_Within(geometry, ST_Buffer(ST_GeomFromText('POINT(50 50)'), 50)) = true"))
            .execute();

    Assert.assertEquals(execute.size(), 2);

    execute =
        db.command(
                new OCommandSQL(
                    "SELECT from Polygon where ST_Within(geometry, ST_Buffer(ST_GeomFromText('POINT(50 50)'), 30)) = true"))
            .execute();

    Assert.assertEquals(execute.size(), 1);
  }

  @Test
  public void testWithinNewExecutor() throws Exception {
    db.command("create class Polygon extends v");
    db.command("create property Polygon.geometry EMBEDDED OPolygon");

    db.command("insert into Polygon set geometry = ST_Buffer(ST_GeomFromText('POINT(50 50)'), 20)");
    db.command("insert into Polygon set geometry = ST_Buffer(ST_GeomFromText('POINT(50 50)'), 40)");

    db.command("create index Polygon.g on Polygon(geometry) SPATIAL ENGINE LUCENE");
    OResultSet execute =
        db.query(
            "SELECT from Polygon where ST_Within(geometry, ST_Buffer(ST_GeomFromText('POINT(50 50)'), 50)) = true");

    assertThat(execute).hasSize(2);

    execute =
        db.query(
            "SELECT from Polygon where ST_Within(geometry, ST_Buffer(ST_GeomFromText('POINT(50 50)'), 30)) = true");

    assertThat(execute).hasSize(1);
  }
}
