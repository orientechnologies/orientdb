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

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import org.junit.Assert;
import org.junit.Test;

public class OCommandExecutorSQLDropPropertyTest extends BaseMemoryDatabase {

  @Test
  public void test() {
    OSchema schema = db.getMetadata().getSchema();
    OClass foo = schema.createClass("Foo");

    foo.createProperty("name", OType.STRING);
    Assert.assertTrue(schema.getClass("Foo").existsProperty("name"));
    db.command("DROP PROPERTY Foo.name").close();
    schema.reload();
    Assert.assertFalse(schema.getClass("Foo").existsProperty("name"));

    foo.createProperty("name", OType.STRING);
    Assert.assertTrue(schema.getClass("Foo").existsProperty("name"));
    db.command("DROP PROPERTY `Foo`.name").close();
    schema.reload();
    Assert.assertFalse(schema.getClass("Foo").existsProperty("name"));

    foo.createProperty("name", OType.STRING);
    Assert.assertTrue(schema.getClass("Foo").existsProperty("name"));
    db.command("DROP PROPERTY Foo.`name`").close();
    schema.reload();
    Assert.assertFalse(schema.getClass("Foo").existsProperty("name"));

    foo.createProperty("name", OType.STRING);
    Assert.assertTrue(schema.getClass("Foo").existsProperty("name"));
    db.command("DROP PROPERTY `Foo`.`name`").close();
    schema.reload();
    Assert.assertFalse(schema.getClass("Foo").existsProperty("name"));
  }

  @Test
  public void testIfExists() {
    OSchema schema = db.getMetadata().getSchema();
    OClass testIfExistsClass = schema.createClass("testIfExists");

    testIfExistsClass.createProperty("name", OType.STRING);
    Assert.assertTrue(schema.getClass("testIfExists").existsProperty("name"));
    db.command("DROP PROPERTY testIfExists.name if exists").close();
    schema.reload();
    Assert.assertFalse(schema.getClass("testIfExists").existsProperty("name"));

    db.command("DROP PROPERTY testIfExists.name if exists").close();
    schema.reload();
    Assert.assertFalse(schema.getClass("testIfExists").existsProperty("name"));
  }
}
