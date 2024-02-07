/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.sql.executor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import org.junit.Assert;
import org.junit.Test;

public class SQLUpdateEdgeTest extends BaseMemoryDatabase {

  @Test
  public void testUpdateEdge() {

    db.command("create class V1 extends V").close();

    db.command("create class E1 extends E").close();

    db.getMetadata().getSchema().reload();

    // VERTEXES
    OElement v1 = db.command("create vertex").next().getElement().get();
    assertEquals(v1.getSchemaType().get().getName(), "V");

    OElement v2 = db.command("create vertex V1").next().getElement().get();
    assertEquals(v2.getSchemaType().get().getName(), "V1");

    OElement v3 =
        db.command("create vertex set vid = 'v3', brand = 'fiat'").next().getElement().get();

    assertEquals(v3.getSchemaType().get().getName(), "V");
    assertEquals(v3.getProperty("brand"), "fiat");

    OElement v4 =
        db.command("create vertex V1 set vid = 'v4',  brand = 'fiat',name = 'wow'")
            .next()
            .getElement()
            .get();
    assertEquals(v4.getSchemaType().get().getName(), "V1");
    assertEquals(v4.getProperty("brand"), "fiat");
    assertEquals(v4.getProperty("name"), "wow");

    OResultSet edges =
        db.command("create edge E1 from " + v1.getIdentity() + " to " + v2.getIdentity());
    OEdge edge = edges.next().getEdge().get();
    assertFalse(edges.hasNext());
    assertEquals(edge.getSchemaType().get().getName(), "E1");

    db.command(
            "update edge E1 set out = "
                + v3.getIdentity()
                + ", in = "
                + v4.getIdentity()
                + " where @rid = "
                + edge.getIdentity())
        .close();

    OResultSet result = db.query("select expand(out('E1')) from " + v3.getIdentity());
    OResult vertex4 = result.next();
    Assert.assertEquals(vertex4.getProperty("vid"), "v4");

    result = db.query("select expand(in('E1')) from " + v4.getIdentity());
    OResult vertex3 = result.next();
    Assert.assertEquals(vertex3.getProperty("vid"), "v3");

    result = db.query("select expand(out('E1')) from " + v1.getIdentity());
    Assert.assertEquals(result.stream().count(), 0);

    result = db.query("select expand(in('E1')) from " + v2.getIdentity());
    Assert.assertEquals(result.stream().count(), 0);
  }

  @Test
  public void testUpdateEdgeOfTypeE() {
    // issue #6378
    OVertex v1 = db.command("create vertex").next().getVertex().get();
    OVertex v2 = db.command("create vertex").next().getVertex().get();
    OVertex v3 = db.command("create vertex").next().getVertex().get();

    OResultSet edges =
        db.command("create edge E from " + v1.getIdentity() + " to " + v2.getIdentity());
    OEdge edge = edges.next().getEdge().get();

    db.command("UPDATE EDGE " + edge.getIdentity() + " SET in = " + v3.getIdentity());

    OResultSet result = db.query("select expand(out()) from " + v1.getIdentity());
    Assert.assertEquals(result.next().getIdentity().get(), v3.getIdentity());

    result = db.query("select expand(in()) from " + v3.getIdentity());
    Assert.assertEquals(result.next().getIdentity().get(), v1.getIdentity());

    result = db.command("select expand(in()) from " + v2.getIdentity());
    Assert.assertFalse(result.hasNext());
  }
}
