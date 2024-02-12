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
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class SQLCreateLinkTest extends DocumentDBBaseTest {
  @Parameters(value = "url")
  public SQLCreateLinkTest(@Optional String url) {
    super(url);
  }

  @Test
  public void createLinktest() {
    database.command("CREATE CLASS POST").close();
    database.command("INSERT INTO POST (id, title) VALUES ( 10, 'NoSQL movement' )").close();
    database.command("INSERT INTO POST (id, title) VALUES ( 20, 'New OrientDB' )").close();

    database.command("INSERT INTO POST (id, title) VALUES ( 30, '(')").close();

    database.command("INSERT INTO POST (id, title) VALUES ( 40, ')')").close();

    database.command("CREATE CLASS COMMENT").close();
    ;
    database.command("INSERT INTO COMMENT (id, postId, text) VALUES ( 0, 10, 'First' )").close();
    database.command("INSERT INTO COMMENT (id, postId, text) VALUES ( 1, 10, 'Second' )").close();
    database.command("INSERT INTO COMMENT (id, postId, text) VALUES ( 21, 10, 'Another' )").close();
    database
        .command("INSERT INTO COMMENT (id, postId, text) VALUES ( 41, 20, 'First again' )")
        .close();
    database
        .command("INSERT INTO COMMENT (id, postId, text) VALUES ( 82, 20, 'Second Again' )")
        .close();

    Assert.assertEquals(
        ((Number)
                database
                    .command(
                        "CREATE LINK comments TYPE LINKSET FROM comment.postId TO post.id"
                            + " INVERSE")
                    .next()
                    .getProperty("count"))
            .intValue(),
        5);

    Assert.assertEquals(
        ((Number) database.command("UPDATE comment REMOVE postId").next().getProperty("count"))
            .intValue(),
        5);
  }

  @Test
  public void createRIDLinktest() {

    database.command("CREATE CLASS POST2").close();
    Object p1 =
        database
            .command("INSERT INTO POST2 (id, title) VALUES ( 10, 'NoSQL movement' )")
            .next()
            .toElement();
    Assert.assertTrue(p1 instanceof ODocument);
    Object p2 =
        database
            .command("INSERT INTO POST2 (id, title) VALUES ( 20, 'New OrientDB' )")
            .next()
            .toElement();
    Assert.assertTrue(p2 instanceof ODocument);

    Object p3 =
        database.command("INSERT INTO POST2 (id, title) VALUES ( 30, '(')").next().toElement();
    Assert.assertTrue(p3 instanceof ODocument);

    Object p4 =
        database.command("INSERT INTO POST2 (id, title) VALUES ( 40, ')')").next().toElement();
    Assert.assertTrue(p4 instanceof ODocument);

    database.command("CREATE CLASS COMMENT2");
    database
        .command(
            "INSERT INTO COMMENT2 (id, postId, text) VALUES ( 0, '"
                + ((ODocument) p1).getIdentity().toString()
                + "', 'First' )")
        .close();
    database
        .command(
            "INSERT INTO COMMENT2 (id, postId, text) VALUES ( 1, '"
                + ((ODocument) p1).getIdentity().toString()
                + "', 'Second' )")
        .close();
    database
        .command(
            "INSERT INTO COMMENT2 (id, postId, text) VALUES ( 21, '"
                + ((ODocument) p1).getIdentity().toString()
                + "', 'Another' )")
        .close();
    database
        .command(
            "INSERT INTO COMMENT2 (id, postId, text) VALUES ( 41, '"
                + ((ODocument) p2).getIdentity().toString()
                + "', 'First again' )")
        .close();
    database
        .command(
            "INSERT INTO COMMENT2 (id, postId, text) VALUES ( 82, '"
                + ((ODocument) p2).getIdentity().toString()
                + "', 'Second Again' )")
        .close();

    Assert.assertEquals(
        ((Number)
                database
                    .command(
                        new OCommandSQL(
                            "CREATE LINK comments TYPE LINKSET FROM comment2.postId TO post2.@rid"
                                + " INVERSE"))
                    .execute())
            .intValue(),
        5);

    Assert.assertEquals(
        ((Number) database.command("UPDATE comment2 REMOVE postId").next().getProperty("count"))
            .intValue(),
        5);
  }
}
