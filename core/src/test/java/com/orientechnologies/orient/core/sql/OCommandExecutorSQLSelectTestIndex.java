/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

package com.orientechnologies.orient.core.sql;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.BaseMemoryInternalDatabase;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.List;
import org.junit.Test;

public class OCommandExecutorSQLSelectTestIndex extends BaseMemoryInternalDatabase {

  @Test
  public void testIndexSqlEmbeddedList() {

    db.command(new OCommandSQL("create class Foo")).execute();
    db.command(new OCommandSQL("create property Foo.bar EMBEDDEDLIST STRING")).execute();
    db.command(new OCommandSQL("create index Foo.bar on Foo (bar) NOTUNIQUE")).execute();
    db.command(new OCommandSQL("insert into Foo set bar = ['yep']")).execute();
    List<ODocument> results =
        db.command(new OCommandSQL("select from Foo where bar = 'yep'")).execute();
    assertEquals(results.size(), 1);

    final OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "Foo.bar");
    assertEquals(index.getInternal().size(), 1);
  }

  @Test
  public void testIndexOnHierarchyChange() {
    // issue #5743

    db.command(new OCommandSQL("CREATE CLASS Main ABSTRACT")).execute();
    db.command(new OCommandSQL("CREATE PROPERTY Main.uuid String")).execute();
    db.command(new OCommandSQL("CREATE INDEX Main.uuid UNIQUE_HASH_INDEX")).execute();
    db.command(new OCommandSQL("CREATE CLASS Base EXTENDS Main ABSTRACT")).execute();
    db.command(new OCommandSQL("CREATE CLASS Derived EXTENDS Main")).execute();
    db.command(new OCommandSQL("INSERT INTO Derived SET uuid='abcdef'")).execute();
    db.command(new OCommandSQL("ALTER CLASS Derived SUPERCLASSES Base")).execute();

    List<ODocument> results =
        db.command(new OCommandSQL("SELECT * FROM Derived WHERE uuid='abcdef'")).execute();
    assertEquals(results.size(), 1);
  }

  @Test
  public void testListContainsField() {

    db.command(new OCommandSQL("CREATE CLASS Foo")).execute();
    db.command(new OCommandSQL("CREATE PROPERTY Foo.name String")).execute();
    db.command(new OCommandSQL("INSERT INTO Foo SET name = 'foo'")).execute();

    List<?> result =
        db.query(
            new OSQLSynchQuery<Object>("SELECT * FROM Foo WHERE ['foo', 'bar'] CONTAINS name"));
    assertEquals(result.size(), 1);

    result = db.query(new OSQLSynchQuery<Object>("SELECT * FROM Foo WHERE name IN ['foo', 'bar']"));
    assertEquals(result.size(), 1);

    db.command(new OCommandSQL("CREATE INDEX Foo.name UNIQUE_HASH_INDEX")).execute();

    result =
        db.query(
            new OSQLSynchQuery<Object>("SELECT * FROM Foo WHERE ['foo', 'bar'] CONTAINS name"));
    assertEquals(result.size(), 1);

    result = db.query(new OSQLSynchQuery<Object>("SELECT * FROM Foo WHERE name IN ['foo', 'bar']"));
    assertEquals(result.size(), 1);
  }
}
