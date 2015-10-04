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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentComparator;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

@Test(groups = { "crud", "record-document" })
public class CRUDDocumentValidationTest extends DocumentDBBaseTest {
  private ODocument record;
  private ODocument account;

  @Parameters(value = "url")
  public CRUDDocumentValidationTest(@Optional String url) {
    super(url);
  }

  @Test
  public void openDb() {
    createBasicTestSchema();

    record = database.newInstance("Whiz");
    account = new ODocument("Account");
    account.field("id", "1234567890");
  }

  @Test(dependsOnMethods = "openDb", expectedExceptions = OValidationException.class)
  public void validationMandatory() {
    record.clear();
    record.save();
  }

  @Test(dependsOnMethods = "validationMandatory", expectedExceptions = OValidationException.class)
  public void validationMinString() {
    record.clear();
    record.field("account", account);
    record.field("id", 23723);
    record.field("text", "");
    record.save();
  }

  @Test(dependsOnMethods = "validationMinString", expectedExceptions = OValidationException.class, expectedExceptionsMessageRegExp = ".*more.*than.*")
  public void validationMaxString() {
    record.clear();
    record.field("account", account);
    record.field("id", 23723);
    record
        .field(
            "text",
            "clfdkkjsd hfsdkjhf fjdkghjkfdhgjdfh gfdgjfdkhgfd skdjaksdjf skdjf sdkjfsd jfkldjfkjsdf kljdk fsdjf kldjgjdhjg khfdjgk hfjdg hjdfhgjkfhdgj kfhdjghrjg");
    record.save();
  }

  @Test(dependsOnMethods = "validationMaxString", expectedExceptions = OValidationException.class, expectedExceptionsMessageRegExp = ".*precedes.*")
  public void validationMinDate() throws ParseException {
    record.clear();
    record.field("account", account);
    record.field("date", new SimpleDateFormat("dd/MM/yyyy").parse("01/33/1976"));
    record.field("text", "test");
    record.save();
  }

  @Test(dependsOnMethods = "validationMinDate", expectedExceptions = OValidationException.class)
  public void validationEmbeddedType() throws ParseException {
    record.clear();
    record.field("account", database.getUser());
    record.save();
  }

  @Test(dependsOnMethods = "validationEmbeddedType", expectedExceptions = OValidationException.class)
  public void validationStrictClass() throws ParseException {
    ODocument doc = new ODocument("StrictTest");
    doc.field("id", 122112);
    doc.field("antani", "122112");
    doc.save();
  }

  @Test(dependsOnMethods = "validationStrictClass")
  public void closeDb() {
    database.close();
  }

  @Test(dependsOnMethods = "closeDb")
  public void createSchemaForMandatoryNullableTest() throws ParseException {
    if (database.getMetadata().getSchema().existsClass("MyTestClass")) {
      database.getMetadata().getSchema().dropClass("MyTestClass");
      database.getMetadata().getSchema().reload();
    }

    database.command(new OCommandSQL("CREATE CLASS MyTestClass")).execute();
    database.command(new OCommandSQL("CREATE PROPERTY MyTestClass.keyField STRING")).execute();
    database.command(new OCommandSQL("ALTER PROPERTY MyTestClass.keyField MANDATORY true")).execute();
    database.command(new OCommandSQL("ALTER PROPERTY MyTestClass.keyField NOTNULL true")).execute();
    database.command(new OCommandSQL("CREATE PROPERTY MyTestClass.dateTimeField DATETIME")).execute();
    database.command(new OCommandSQL("ALTER PROPERTY MyTestClass.dateTimeField MANDATORY true")).execute();
    database.command(new OCommandSQL("ALTER PROPERTY MyTestClass.dateTimeField NOTNULL false")).execute();
    database.command(new OCommandSQL("CREATE PROPERTY MyTestClass.stringField STRING")).execute();
    database.command(new OCommandSQL("ALTER PROPERTY MyTestClass.stringField MANDATORY true")).execute();
    database.command(new OCommandSQL("ALTER PROPERTY MyTestClass.stringField NOTNULL false")).execute();
    database.command(new OCommandSQL("INSERT INTO MyTestClass (keyField,dateTimeField,stringField) VALUES (\"K1\",null,null)"))
        .execute();
    database.reload();
    database.getStorage().reload();
    database.getMetadata().reload();
    database.close();
    database.open("admin", "admin");
    OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>("SELECT FROM MyTestClass WHERE keyField = ?");
    List<ODocument> result = database.query(query, "K1");
    Assert.assertEquals(1, result.size());
    ODocument doc = result.get(0);
    Assert.assertTrue(doc.containsField("keyField"));
    Assert.assertTrue(doc.containsField("dateTimeField"));
    Assert.assertTrue(doc.containsField("stringField"));
  }

  @Test(dependsOnMethods = "createSchemaForMandatoryNullableTest")
  public void testUpdateDocDefined() {
    OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>("SELECT FROM MyTestClass WHERE keyField = ?");
    List<ODocument> result = database.query(query, "K1");
    Assert.assertEquals(1, result.size());
    ODocument doc = result.get(0);
    doc.field("keyField", "K1N");
    doc.save();
  }

  @Test(dependsOnMethods = "testUpdateDocDefined")
  public void validationMandatoryNullableCloseDb() throws ParseException {
    ODocument doc = new ODocument("MyTestClass");
    doc.field("keyField", "K2");
    doc.field("dateTimeField", (Date) null);
    doc.field("stringField", (String) null);
    doc.save();

    database.close();
    database.open("admin", "admin");

    OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>("SELECT FROM MyTestClass WHERE keyField = ?");
    List<ODocument> result = database.query(query, "K2");
    Assert.assertEquals(1, result.size());
    doc = result.get(0);
    doc.field("keyField", "K2N");
    doc.save();
  }

  @Test(dependsOnMethods = "validationMandatoryNullableCloseDb")
  public void validationMandatoryNullableNoCloseDb() throws ParseException {
    ODocument doc = new ODocument("MyTestClass");
    doc.field("keyField", "K3");
    doc.field("dateTimeField", (Date) null);
    doc.field("stringField", (String) null);
    doc.save();

    OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>("SELECT FROM MyTestClass WHERE keyField = ?");
    List<ODocument> result = database.query(query, "K3");
    Assert.assertEquals(1, result.size());
    doc = result.get(0);
    doc.field("keyField", "K3N");
    doc.save();
  }

  @Test(dependsOnMethods = "validationMandatoryNullableNoCloseDb")
  public void validationDisabledAdDatabaseLevel() throws ParseException {
    database.getMetadata().reload();
    try {
      ODocument doc = new ODocument("MyTestClass");
      doc.save();
      Assert.fail();
    } catch (OValidationException e) {
    }

    database.command(new OCommandSQL("ALTER DATABASE " + ODatabase.ATTRIBUTES.VALIDATION.name() + " FALSE")).execute();
    try {

      ODocument doc = new ODocument("MyTestClass");
      doc.save();

      doc.delete();
    } finally {
      database.command(new OCommandSQL("ALTER DATABASE " + ODatabase.ATTRIBUTES.VALIDATION.name() + " TRUE")).execute();
    }
  }

  @Test(dependsOnMethods = "validationDisabledAdDatabaseLevel")
  public void dropSchemaForMandatoryNullableTest() throws ParseException {
    database.command(new OCommandSQL("DROP CLASS MyTestClass")).execute();
    database.getMetadata().reload();
  }

  @Test
  public void testNullComparison() {
    // given
    ODocument doc1 = new ODocument().field("testField", (Object) null);
    ODocument doc2 = new ODocument().field("testField", (Object) null);

    ODocumentComparator comparator = new ODocumentComparator(
        Collections.singletonList(new OPair<String, String>("testField", "asc")), new OBasicCommandContext());

    Assert.assertEquals(comparator.compare(doc1, doc2), 0);
  }
}
