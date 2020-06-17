/*
 *
 *  *  Copyright 2016 OrientDB LTD (info(at)orientdb.com)
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
 *  * For more information: http://www.orientdb.com
 */

package com.orientechnologies.orient.core.index;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OLegacyResultSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** @author Sergey Sitnikov */
public class TxNonUniqueIndexWithCollationTest {

  private ODatabaseDocumentTx db;

  @Before
  public void before() {
    db = new ODatabaseDocumentTx("memory:TxNonUniqueIndexWithCollationTest");
    db.create();
    db.getMetadata()
        .getSchema()
        .createClass("user")
        .createProperty("name", OType.STRING)
        .setCollate("ci")
        .createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

    db.newInstance("user").field("name", "abc").save();
    db.newInstance("user").field("name", "aby").save();
    db.newInstance("user").field("name", "aby").save();
    db.newInstance("user").field("name", "abz").save();
  }

  @After
  public void after() {
    db.drop();
  }

  @Test
  public void testSubstrings() {
    db.begin();

    db.command(new OCommandSQL("update user set name='abd' where name='Aby'")).execute();

    final OLegacyResultSet<ODocument> r =
        db.command(new OCommandSQL("select * from user where name like '%B%' order by name"))
            .execute();
    assertEquals(4, r.size());
    assertEquals("abc", r.get(0).field("name"));
    assertEquals("abd", r.get(1).field("name"));
    assertEquals("abd", r.get(2).field("name"));
    assertEquals("abz", r.get(3).field("name"));

    db.commit();
  }

  @Test
  public void testRange() {
    db.begin();

    db.command(new OCommandSQL("update user set name='Abd' where name='Aby'")).execute();

    final OLegacyResultSet<ODocument> r =
        db.command(new OCommandSQL("select * from user where name >= 'abd' order by name"))
            .execute();
    assertEquals(3, r.size());
    assertEquals("Abd", r.get(0).field("name"));
    assertEquals("Abd", r.get(1).field("name"));
    assertEquals("abz", r.get(2).field("name"));

    db.commit();
  }

  @Test
  public void testIn() {
    db.begin();

    db.command(new OCommandSQL("update user set name='abd' where name='Aby'")).execute();

    final OLegacyResultSet<ODocument> r =
        db.command(
                new OCommandSQL(
                    "select * from user where name in ['Abc', 'Abd', 'Abz'] order by name"))
            .execute();
    assertEquals(4, r.size());
    assertEquals("abc", r.get(0).field("name"));
    assertEquals("abd", r.get(1).field("name"));
    assertEquals("abd", r.get(2).field("name"));
    assertEquals("abz", r.get(3).field("name"));

    db.commit();
  }
}
