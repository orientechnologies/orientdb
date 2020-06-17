/*
 *
 *  *  Copyright 2015 OrientDB LTD (info(at)orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.sql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class OCommandExecutorSQLCreateSequenceTest {
  static ODatabaseDocumentTx db;
  private static String DB_STORAGE = "memory";
  private static String DB_NAME = "OCommandExecutorSQLCreateSequenceTest";

  @BeforeClass
  public static void beforeClass() throws Exception {
    db = new ODatabaseDocumentTx(DB_STORAGE + ":" + DB_NAME);
    db.create();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (db.isClosed()) {
      db.open("admin", "admin");
    }
    db.command(new OCommandSQL("drop class foo")).execute();
    db.getMetadata().getSchema().reload();
    db.close();
  }

  @Test
  public void testSimple() {
    db.command(new OCommandSQL("CREATE SEQUENCE Sequence1 TYPE ORDERED")).execute();

    List<ODocument> results =
        db.query(new OSQLSynchQuery("select sequence('Sequence1').next() as val"));
    assertEquals(results.size(), 1);
    for (ODocument result : results) {
      assertThat(result.<Long>field("val")).isEqualTo(1L);
    }
    results = db.query(new OSQLSynchQuery("select sequence('Sequence1').next() as val"));
    assertEquals(results.size(), 1);
    for (ODocument result : results) {
      assertThat(result.<Long>field("val")).isEqualTo(2L);
    }
    results = db.query(new OSQLSynchQuery("select sequence('Sequence1').next() as val"));
    assertEquals(results.size(), 1);
    for (ODocument result : results) {
      assertThat(result.<Long>field("val")).isEqualTo(3L);
    }
  }

  @Test
  public void testIncrement() {
    db.command(new OCommandSQL("CREATE SEQUENCE SequenceIncrement TYPE ORDERED INCREMENT 3"))
        .execute();

    List<ODocument> results =
        db.query(new OSQLSynchQuery("select sequence('SequenceIncrement').next() as val"));
    assertEquals(results.size(), 1);
    for (ODocument result : results) {
      assertThat(result.<Long>field("val")).isEqualTo(3L);
    }
    results = db.query(new OSQLSynchQuery("select sequence('SequenceIncrement').next() as val"));
    assertEquals(results.size(), 1);
    for (ODocument result : results) {
      assertThat(result.<Long>field("val")).isEqualTo(6L);
    }
    results = db.query(new OSQLSynchQuery("select sequence('SequenceIncrement').next() as val"));
    assertEquals(results.size(), 1);
    for (ODocument result : results) {
      assertThat(result.<Long>field("val")).isEqualTo(9L);
    }
  }

  @Test
  public void testStart() {
    db.command(new OCommandSQL("CREATE SEQUENCE SequenceStart TYPE ORDERED START 3")).execute();

    List<ODocument> results =
        db.query(new OSQLSynchQuery("select sequence('SequenceStart').next() as val"));
    assertEquals(results.size(), 1);
    for (ODocument result : results) {
      assertThat(result.<Long>field("val")).isEqualTo(4L);
    }
    results = db.query(new OSQLSynchQuery("select sequence('SequenceStart').next() as val"));
    assertEquals(results.size(), 1);
    for (ODocument result : results) {
      assertThat(result.<Long>field("val")).isEqualTo(5L);
    }
    results = db.query(new OSQLSynchQuery("select sequence('SequenceStart').next() as val"));
    assertEquals(results.size(), 1);
    for (ODocument result : results) {
      assertThat(result.<Long>field("val")).isEqualTo(6L);
    }
  }

  @Test
  public void testStartIncrement() {
    db.command(
            new OCommandSQL(
                "CREATE SEQUENCE SequenceStartIncrement TYPE ORDERED START 3 INCREMENT 10"))
        .execute();

    List<ODocument> results =
        db.query(new OSQLSynchQuery("select sequence('SequenceStartIncrement').next() as val"));
    assertEquals(results.size(), 1);
    for (ODocument result : results) {
      assertThat(result.<Long>field("val")).isEqualTo(13L);
    }
    results =
        db.query(new OSQLSynchQuery("select sequence('SequenceStartIncrement').next() as val"));
    assertEquals(results.size(), 1);
    for (ODocument result : results) {
      assertThat(result.<Long>field("val")).isEqualTo(23L);
    }
    results =
        db.query(new OSQLSynchQuery("select sequence('SequenceStartIncrement').next() as val"));
    assertEquals(results.size(), 1);
    for (ODocument result : results) {
      assertThat(result.<Long>field("val")).isEqualTo(33L);
    }
  }
}
