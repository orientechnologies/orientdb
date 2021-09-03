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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.client.db.ODatabaseHelper;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "schema")
public class AbstractClassTest extends DocumentDBBaseTest {
  @Parameters(value = "url")
  public AbstractClassTest(@Optional String url) {
    super(url);
  }

  @BeforeClass
  public void createSchema() throws IOException {
    database = new ODatabaseDocumentTx(url);
    if (ODatabaseHelper.existsDatabase(database, "plocal")) database.open("admin", "admin");
    else database.create();

    OClass abstractPerson =
        database.getMetadata().getSchema().createAbstractClass("AbstractPerson");
    abstractPerson.createProperty("name", OType.STRING);

    Assert.assertTrue(abstractPerson.isAbstract());
    Assert.assertEquals(abstractPerson.getClusterIds().length, 1);
    Assert.assertEquals(abstractPerson.getDefaultClusterId(), -1);
  }

  @Test
  public void testCannotCreateInstances() {
    try {
      new ODocument("AbstractPerson").save();
    } catch (OException e) {
      Throwable cause = e;

      while (cause.getCause() != null) cause = cause.getCause();

      Assert.assertTrue(cause instanceof OSchemaException);
    }
  }
}
