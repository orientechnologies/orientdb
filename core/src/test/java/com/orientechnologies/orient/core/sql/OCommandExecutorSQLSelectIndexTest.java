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
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Test;

public class OCommandExecutorSQLSelectIndexTest extends BaseMemoryInternalDatabase {

  @Test
  public void testIndexSqlEmbeddedList() {

    db.command("create class Foo").close();
    db.command("create property Foo.bar EMBEDDEDLIST STRING").close();
    db.command("create index Foo.bar on Foo (bar) NOTUNIQUE").close();
    db.command("insert into Foo set bar = ['yep']").close();
    OResultSet results = db.query("select from Foo where bar = 'yep'");
    assertEquals(results.stream().count(), 1);

    final OIndex index =
        ((OMetadataDefault) db.getMetadata())
            .getIndexManagerInternal()
            .getIndex((ODatabaseDocumentInternal) db, "Foo.bar");
    assertEquals(index.getInternal().size(), 1);
  }

  @Test
  public void testIndexOnHierarchyChange() {
    // issue #5743

    db.command("CREATE CLASS Main ABSTRACT").close();
    db.command("CREATE PROPERTY Main.uuid String").close();
    db.command("CREATE INDEX Main.uuid UNIQUE_HASH_INDEX").close();
    db.command("CREATE CLASS Base EXTENDS Main ABSTRACT").close();
    db.command("CREATE CLASS Derived EXTENDS Main").close();
    db.command("INSERT INTO Derived SET uuid='abcdef'").close();
    db.command("ALTER CLASS Derived SUPERCLASSES Base").close();

    OResultSet results = db.query("SELECT * FROM Derived WHERE uuid='abcdef'");
    assertEquals(results.stream().count(), 1);
  }

  @Test
  public void testListContainsField() {
    db.command("CREATE CLASS Foo").close();
    db.command("CREATE PROPERTY Foo.name String").close();
    db.command("INSERT INTO Foo SET name = 'foo'").close();

    OResultSet result = db.query("SELECT * FROM Foo WHERE ['foo', 'bar'] CONTAINS name");
    assertEquals(result.stream().count(), 1);

    result = db.query("SELECT * FROM Foo WHERE name IN ['foo', 'bar']");
    assertEquals(result.stream().count(), 1);

    db.command("CREATE INDEX Foo.name UNIQUE_HASH_INDEX").close();

    result = db.query("SELECT * FROM Foo WHERE ['foo', 'bar'] CONTAINS name");
    assertEquals(result.stream().count(), 1);

    result = db.query("SELECT * FROM Foo WHERE name IN ['foo', 'bar']");
    assertEquals(result.stream().count(), 1);

    result = db.query("SELECT * FROM Foo WHERE name IN ['foo', 'bar']");
    assertEquals(result.stream().count(), 1);
  }
}
