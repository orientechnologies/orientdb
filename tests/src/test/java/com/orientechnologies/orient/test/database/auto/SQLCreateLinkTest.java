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
    Assert.assertTrue(
        (Integer) database.command(new OCommandSQL("CREATE CLASS POST")).execute() > 0);
    Assert.assertTrue(
        database
                .command(
                    new OCommandSQL("INSERT INTO POST (id, title) VALUES ( 10, 'NoSQL movement' )"))
                .execute()
            instanceof ODocument);
    Assert.assertTrue(
        database
                .command(
                    new OCommandSQL("INSERT INTO POST (id, title) VALUES ( 20, 'New OrientDB' )"))
                .execute()
            instanceof ODocument);

    Assert.assertTrue(
        database
                .command(new OCommandSQL("INSERT INTO POST (id, title) VALUES ( 30, '(')"))
                .execute()
            instanceof ODocument);

    Assert.assertTrue(
        database
                .command(new OCommandSQL("INSERT INTO POST (id, title) VALUES ( 40, ')')"))
                .execute()
            instanceof ODocument);

    Assert.assertTrue(
        (Integer) database.command(new OCommandSQL("CREATE CLASS COMMENT")).execute() > 0);
    Assert.assertTrue(
        database
                .command(
                    new OCommandSQL(
                        "INSERT INTO COMMENT (id, postId, text) VALUES ( 0, 10, 'First' )"))
                .execute()
            instanceof ODocument);
    Assert.assertTrue(
        database
                .command(
                    new OCommandSQL(
                        "INSERT INTO COMMENT (id, postId, text) VALUES ( 1, 10, 'Second' )"))
                .execute()
            instanceof ODocument);
    Assert.assertTrue(
        database
                .command(
                    new OCommandSQL(
                        "INSERT INTO COMMENT (id, postId, text) VALUES ( 21, 10, 'Another' )"))
                .execute()
            instanceof ODocument);
    Assert.assertTrue(
        database
                .command(
                    new OCommandSQL(
                        "INSERT INTO COMMENT (id, postId, text) VALUES ( 41, 20, 'First again' )"))
                .execute()
            instanceof ODocument);
    Assert.assertTrue(
        database
                .command(
                    new OCommandSQL(
                        "INSERT INTO COMMENT (id, postId, text) VALUES ( 82, 20, 'Second Again' )"))
                .execute()
            instanceof ODocument);

    Assert.assertEquals(
        ((Number)
                database
                    .command(
                        new OCommandSQL(
                            "CREATE LINK comments TYPE LINKSET FROM comment.postId TO post.id INVERSE"))
                    .execute())
            .intValue(),
        5);

    Assert.assertEquals(
        ((Number) database.command(new OCommandSQL("UPDATE comment REMOVE postId")).execute())
            .intValue(),
        5);
  }

  @Test
  public void createRIDLinktest() {
    Assert.assertTrue(
        (Integer) database.command(new OCommandSQL("CREATE CLASS POST2")).execute() > 0);
    Object p1 =
        database
            .command(
                new OCommandSQL("INSERT INTO POST2 (id, title) VALUES ( 10, 'NoSQL movement' )"))
            .execute();
    Assert.assertTrue(p1 instanceof ODocument);
    Object p2 =
        database
            .command(new OCommandSQL("INSERT INTO POST2 (id, title) VALUES ( 20, 'New OrientDB' )"))
            .execute();
    Assert.assertTrue(p2 instanceof ODocument);

    Object p3 =
        database
            .command(new OCommandSQL("INSERT INTO POST2 (id, title) VALUES ( 30, '(')"))
            .execute();
    Assert.assertTrue(p3 instanceof ODocument);

    Object p4 =
        database
            .command(new OCommandSQL("INSERT INTO POST2 (id, title) VALUES ( 40, ')')"))
            .execute();
    Assert.assertTrue(p4 instanceof ODocument);

    Assert.assertTrue(
        (Integer) database.command(new OCommandSQL("CREATE CLASS COMMENT2")).execute() > 0);
    Assert.assertTrue(
        database
                .command(
                    new OCommandSQL(
                        "INSERT INTO COMMENT2 (id, postId, text) VALUES ( 0, '"
                            + ((ODocument) p1).getIdentity().toString()
                            + "', 'First' )"))
                .execute()
            instanceof ODocument);
    Assert.assertTrue(
        database
                .command(
                    new OCommandSQL(
                        "INSERT INTO COMMENT2 (id, postId, text) VALUES ( 1, '"
                            + ((ODocument) p1).getIdentity().toString()
                            + "', 'Second' )"))
                .execute()
            instanceof ODocument);
    Assert.assertTrue(
        database
                .command(
                    new OCommandSQL(
                        "INSERT INTO COMMENT2 (id, postId, text) VALUES ( 21, '"
                            + ((ODocument) p1).getIdentity().toString()
                            + "', 'Another' )"))
                .execute()
            instanceof ODocument);
    Assert.assertTrue(
        database
                .command(
                    new OCommandSQL(
                        "INSERT INTO COMMENT2 (id, postId, text) VALUES ( 41, '"
                            + ((ODocument) p2).getIdentity().toString()
                            + "', 'First again' )"))
                .execute()
            instanceof ODocument);
    Assert.assertTrue(
        database
                .command(
                    new OCommandSQL(
                        "INSERT INTO COMMENT2 (id, postId, text) VALUES ( 82, '"
                            + ((ODocument) p2).getIdentity().toString()
                            + "', 'Second Again' )"))
                .execute()
            instanceof ODocument);

    Assert.assertEquals(
        ((Number)
                database
                    .command(
                        new OCommandSQL(
                            "CREATE LINK comments TYPE LINKSET FROM comment2.postId TO post2.@rid INVERSE"))
                    .execute())
            .intValue(),
        5);

    Assert.assertEquals(
        ((Number) database.command(new OCommandSQL("UPDATE comment2 REMOVE postId")).execute())
            .intValue(),
        5);
  }
}
