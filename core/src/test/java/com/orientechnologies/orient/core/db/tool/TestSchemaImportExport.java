package com.orientechnologies.orient.core.db.tool;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

public class TestSchemaImportExport {

  @Test
  public void testExportImportCustomData() throws IOException {
    OrientDB orientdb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
    orientdb.execute(
        "create database "
            + TestSchemaImportExport.class.getSimpleName()
            + " memory users(admin identified by 'adminpwd' role admin)");

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try (ODatabaseDocument db =
        orientdb.open(TestSchemaImportExport.class.getSimpleName(), "admin", "adminpwd")) {
      OClass clazz = db.getMetadata().getSchema().createClass("Test");
      clazz.createProperty("some", OType.STRING);
      clazz.setCustom("testcustom", "test");
      ODatabaseExport exp =
          new ODatabaseExport((ODatabaseDocumentInternal) db, output, new MockOutputListener());
      exp.exportDatabase();
    } finally {
      orientdb.drop(TestSchemaImportExport.class.getSimpleName());
    }

    orientdb.execute(
        "create database imp_"
            + TestSchemaImportExport.class.getSimpleName()
            + " memory users(admin identified by 'adminpwd' role admin)");
    try (ODatabaseDocument db1 =
        orientdb.open("imp_" + TestSchemaImportExport.class.getSimpleName(), "admin", "adminpwd")) {
      ODatabaseImport imp =
          new ODatabaseImport(
              (ODatabaseDocumentInternal) db1,
              new ByteArrayInputStream(output.toByteArray()),
              new MockOutputListener());
      imp.importDatabase();
    }
    try (ODatabaseDocument db1 =
        orientdb.open("imp_" + TestSchemaImportExport.class.getSimpleName(), "admin", "adminpwd")) {
      OClass clas1 = db1.getMetadata().getSchema().getClass("Test");
      Assert.assertNotNull(clas1);
      Assert.assertEquals(clas1.getCustom("testcustom"), "test");
    } finally {
      orientdb.drop("imp_" + TestSchemaImportExport.class.getSimpleName());
    }
    orientdb.close();
  }

  @Test
  public void testExportImportDefaultValue() throws IOException {
    OrientDB orientdb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
    orientdb.execute(
        "create database "
            + TestSchemaImportExport.class.getSimpleName()
            + " memory users(admin identified by 'adminpwd' role admin)");

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try (ODatabaseDocument db =
        orientdb.open(TestSchemaImportExport.class.getSimpleName(), "admin", "adminpwd")) {
      OClass clazz = db.getMetadata().getSchema().createClass("Test");
      clazz.createProperty("bla", OType.STRING).setDefaultValue("something");
      ODatabaseExport exp =
          new ODatabaseExport((ODatabaseDocumentInternal) db, output, new MockOutputListener());
      exp.exportDatabase();
    } finally {
      orientdb.drop(TestSchemaImportExport.class.getSimpleName());
    }

    orientdb.execute(
        "create database imp_"
            + TestSchemaImportExport.class.getSimpleName()
            + " memory users(admin identified by 'adminpwd' role admin)");
    try (ODatabaseDocument db1 =
        orientdb.open("imp_" + TestSchemaImportExport.class.getSimpleName(), "admin", "adminpwd")) {
      ODatabaseImport imp =
          new ODatabaseImport(
              (ODatabaseDocumentInternal) db1,
              new ByteArrayInputStream(output.toByteArray()),
              new MockOutputListener());
      imp.importDatabase();
    }
    try (ODatabaseDocument db1 =
        orientdb.open("imp_" + TestSchemaImportExport.class.getSimpleName(), "admin", "adminpwd")) {
      OClass clas1 = db1.getMetadata().getSchema().getClass("Test");
      Assert.assertNotNull(clas1);
      OProperty prop1 = clas1.getProperty("bla");
      Assert.assertNotNull(prop1);
      Assert.assertEquals(prop1.getDefaultValue(), "something");
    } finally {
      orientdb.drop("imp_" + TestSchemaImportExport.class.getSimpleName());
    }
    orientdb.close();
  }

  @Test
  public void testExportImportMultipleInheritance() throws IOException {
    OrientDB orientdb = new OrientDB("memory:", OrientDBConfig.defaultConfig());
    orientdb.execute(
        "create database "
            + TestSchemaImportExport.class.getSimpleName()
            + "MultipleInheritance memory users(admin identified by 'adminpwd' role admin)");

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try (ODatabaseDocument db =
        orientdb.open(
            TestSchemaImportExport.class.getSimpleName() + "MultipleInheritance",
            "admin",
            "adminpwd")) {
      OClass clazz = db.getMetadata().getSchema().createClass("Test");
      clazz.addSuperClass(db.getMetadata().getSchema().getClass("ORestricted"));
      clazz.addSuperClass(db.getMetadata().getSchema().getClass("OIdentity"));

      ODatabaseExport exp =
          new ODatabaseExport((ODatabaseDocumentInternal) db, output, new MockOutputListener());
      exp.exportDatabase();
    } finally {
      orientdb.drop(TestSchemaImportExport.class.getSimpleName() + "MultipleInheritance");
    }
    orientdb.execute(
        "create database imp_"
            + TestSchemaImportExport.class.getSimpleName()
            + "MultipleInheritance memory users(admin identified by 'adminpwd' role admin)");

    try (ODatabaseDocument db1 =
        orientdb.open(
            "imp_" + TestSchemaImportExport.class.getSimpleName() + "MultipleInheritance",
            "admin",
            "adminpwd")) {
      ODatabaseImport imp =
          new ODatabaseImport(
              (ODatabaseDocumentInternal) db1,
              new ByteArrayInputStream(output.toByteArray()),
              new MockOutputListener());
      imp.importDatabase();
    }
    try (ODatabaseDocument db1 =
        orientdb.open(
            "imp_" + TestSchemaImportExport.class.getSimpleName() + "MultipleInheritance",
            "admin",
            "adminpwd")) {
      OClass clas1 = db1.getMetadata().getSchema().getClass("Test");
      Assert.assertTrue(clas1.isSubClassOf("OIdentity"));
      Assert.assertTrue(clas1.isSubClassOf("ORestricted"));
    } finally {
      orientdb.drop("imp_" + TestSchemaImportExport.class.getSimpleName() + "MultipleInheritance");
    }
    orientdb.close();
  }

  private static final class MockOutputListener implements OCommandOutputListener {
    @Override
    public void onMessage(String iText) {}
  }
}
