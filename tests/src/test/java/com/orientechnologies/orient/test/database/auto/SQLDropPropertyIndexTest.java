/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import java.util.Arrays;

import org.testng.Assert;
import org.testng.annotations.*;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.index.OCompositeIndexDefinition;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.enterprise.channel.binary.OResponseProcessingException;

@Test(groups = { "index" })
public class SQLDropPropertyIndexTest extends DocumentDBBaseTest {

  private static final OType        EXPECTED_PROP1_TYPE = OType.DOUBLE;
  private static final OType        EXPECTED_PROP2_TYPE = OType.INTEGER;

	@Parameters(value = "url")
	public SQLDropPropertyIndexTest(@Optional String url) {
		super(url);
	}


  @BeforeMethod
  public void beforeMethod() throws Exception {
    super.beforeMethod();

    final OSchema schema = database.getMetadata().getSchema();
    final OClass oClass = schema.createClass("DropPropertyIndexTestClass");
    oClass.createProperty("prop1", EXPECTED_PROP1_TYPE);
    oClass.createProperty("prop2", EXPECTED_PROP2_TYPE);
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    database.command(new OCommandSQL("drop class DropPropertyIndexTestClass")).execute();
    database.getMetadata().getSchema().reload();

		super.afterMethod();
  }

  @Test
  public void testForcePropertyEnabled() throws Exception {
    database.command(
        new OCommandSQL("CREATE INDEX DropPropertyIndexCompositeIndex ON DropPropertyIndexTestClass (prop2, prop1) UNIQUE"))
        .execute();
    database.getMetadata().getIndexManager().reload();

    OIndex<?> index = database.getMetadata().getSchema().getClass("DropPropertyIndexTestClass")
        .getClassIndex("DropPropertyIndexCompositeIndex");
    Assert.assertNotNull(index);

    database.command(new OCommandSQL("DROP PROPERTY DropPropertyIndexTestClass.prop1 FORCE")).execute();
    database.getMetadata().getIndexManager().reload();

    index = database.getMetadata().getSchema().getClass("DropPropertyIndexTestClass")
        .getClassIndex("DropPropertyIndexCompositeIndex");

    Assert.assertNull(index);
  }

  @Test
  public void testForcePropertyEnabledBrokenCase() throws Exception {
    database.command(
        new OCommandSQL("CREATE INDEX DropPropertyIndexCompositeIndex ON DropPropertyIndexTestClass (prop2, prop1) UNIQUE"))
        .execute();
    database.getMetadata().getIndexManager().reload();

    OIndex<?> index = database.getMetadata().getSchema().getClass("DropPropertyIndexTestClass")
        .getClassIndex("DropPropertyIndexCompositeIndex");
    Assert.assertNotNull(index);

    database.command(new OCommandSQL("DROP PROPERTY DropPropertyIndextestclasS.pRoP1 FORCE")).execute();
    database.getMetadata().getIndexManager().reload();

    index = database.getMetadata().getSchema().getClass("DropPropertyIndexTestClass")
        .getClassIndex("DropPropertyIndexCompositeIndex");

    Assert.assertNull(index);
  }

  @Test
  public void testForcePropertyDisabled() throws Exception {
    database.command(
        new OCommandSQL("CREATE INDEX DropPropertyIndexCompositeIndex ON DropPropertyIndexTestClass (prop1, prop2) UNIQUE"))
        .execute();
    database.getMetadata().getIndexManager().reload();

    OIndex<?> index = database.getMetadata().getSchema().getClass("DropPropertyIndexTestClass")
        .getClassIndex("DropPropertyIndexCompositeIndex");
    Assert.assertNotNull(index);

    try {
      database.command(new OCommandSQL("DROP PROPERTY DropPropertyIndexTestClass.prop1")).execute();
      Assert.fail();
    } catch (OResponseProcessingException e) {
      Assert.assertTrue(e.getCause() instanceof OCommandExecutionException);

      OCommandExecutionException exception = (OCommandExecutionException) e.getCause();
      Assert.assertEquals(exception.getMessage(), "Property used in indexes (" + "DropPropertyIndexCompositeIndex"
          + "). Please drop these indexes before removing property or use FORCE parameter.");
    } catch (OCommandExecutionException e) {
      Assert.assertEquals(e.getMessage(), "Property used in indexes (" + "DropPropertyIndexCompositeIndex"
          + "). Please drop these indexes before removing property or use FORCE parameter.");
    }

    database.getMetadata().getIndexManager().reload();

    index = database.getMetadata().getSchema().getClass("DropPropertyIndexTestClass")
        .getClassIndex("DropPropertyIndexCompositeIndex");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OCompositeIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), Arrays.asList("prop1", "prop2"));
    Assert.assertEquals(indexDefinition.getTypes(), new OType[] { EXPECTED_PROP1_TYPE, EXPECTED_PROP2_TYPE });
    Assert.assertEquals(index.getType(), "UNIQUE");
  }

  @Test
  public void testForcePropertyDisabledBrokenCase() throws Exception {
    database.command(
        new OCommandSQL("CREATE INDEX DropPropertyIndexCompositeIndex ON DropPropertyIndexTestClass (prop1, prop2) UNIQUE"))
        .execute();

    try {
      database.command(new OCommandSQL("DROP PROPERTY DropPropertyIndextestclaSS.proP1")).execute();
      Assert.fail();
    } catch (OResponseProcessingException e) {
      Assert.assertTrue(e.getCause() instanceof OCommandExecutionException);
      OCommandExecutionException exception = (OCommandExecutionException) e.getCause();
      Assert.assertEquals(exception.getMessage(), "Property used in indexes (" + "DropPropertyIndexCompositeIndex"
          + "). Please drop these indexes before removing property or use FORCE parameter.");
    } catch (OCommandExecutionException e) {
      Assert.assertEquals(e.getMessage(), "Property used in indexes (" + "DropPropertyIndexCompositeIndex"
          + "). Please drop these indexes before removing property or use FORCE parameter.");
    }

    database.getMetadata().getIndexManager().reload();
    final OIndex<?> index = database.getMetadata().getSchema().getClass("DropPropertyIndexTestClass")
        .getClassIndex("DropPropertyIndexCompositeIndex");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OCompositeIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), Arrays.asList("prop1", "prop2"));
    Assert.assertEquals(indexDefinition.getTypes(), new OType[] { EXPECTED_PROP1_TYPE, EXPECTED_PROP2_TYPE });
    Assert.assertEquals(index.getType(), "UNIQUE");
  }
}
