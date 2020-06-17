package com.orientechnologies.orient.core.db.hook;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.Assert;
import org.junit.Test;

public class HookChangeValidationTest {

  @Test
  public void testHookCreateChange() {

    ODatabaseDocument db =
        new ODatabaseDocumentTx("memory:" + HookChangeValidationTest.class.getSimpleName());
    db.create();
    try {
      OSchema schema = db.getMetadata().getSchema();
      OClass classA = schema.createClass("TestClass");
      classA.createProperty("property1", OType.STRING).setNotNull(true);
      classA.createProperty("property2", OType.STRING).setReadonly(true);
      classA.createProperty("property3", OType.STRING).setMandatory(true);
      db.registerHook(
          new ODocumentHookAbstract() {
            @Override
            public RESULT onRecordBeforeCreate(ODocument doc) {
              doc.removeField("property1");
              doc.removeField("property2");
              doc.removeField("property3");
              return RESULT.RECORD_CHANGED;
            }

            @Override
            public RESULT onRecordBeforeUpdate(ODocument doc) {
              return RESULT.RECORD_NOT_CHANGED;
            }

            @Override
            public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
              return DISTRIBUTED_EXECUTION_MODE.SOURCE_NODE;
            }
          });
      ODocument doc = new ODocument(classA);
      doc.field("property1", "value1-create");
      doc.field("property2", "value2-create");
      doc.field("property3", "value3-create");
      try {
        doc.save();
        Assert.fail("The document save should fail for validation exception");
      } catch (OValidationException ex) {

      }
    } finally {
      db.drop();
    }
  }

  @Test
  public void testHookUpdateChange() {

    ODatabaseDocument db =
        new ODatabaseDocumentTx("memory:" + HookChangeValidationTest.class.getSimpleName());
    db.create();
    try {
      OSchema schema = db.getMetadata().getSchema();
      OClass classA = schema.createClass("TestClass");
      classA.createProperty("property1", OType.STRING).setNotNull(true);
      classA.createProperty("property2", OType.STRING).setReadonly(true);
      classA.createProperty("property3", OType.STRING).setMandatory(true);
      db.registerHook(
          new ODocumentHookAbstract() {
            @Override
            public RESULT onRecordBeforeCreate(ODocument doc) {
              return RESULT.RECORD_NOT_CHANGED;
            }

            @Override
            public RESULT onRecordBeforeUpdate(ODocument doc) {
              doc.removeField("property1");
              doc.removeField("property2");
              doc.removeField("property3");
              return RESULT.RECORD_CHANGED;
            }

            @Override
            public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
              return DISTRIBUTED_EXECUTION_MODE.SOURCE_NODE;
            }
          });
      ODocument doc = new ODocument(classA);
      doc.field("property1", "value1-create");
      doc.field("property2", "value2-create");
      doc.field("property3", "value3-create");
      doc.save();
      assertEquals("value1-create", doc.field("property1"));
      assertEquals("value2-create", doc.field("property2"));
      assertEquals("value3-create", doc.field("property3"));

      doc.field("property1", "value1-update");
      doc.field("property2", "value2-update");
      try {
        doc.save();
        Assert.fail("The document save should fail for validation exception");
      } catch (OValidationException ex) {

      }
    } finally {
      db.drop();
    }
  }

  @Test
  public void testHookCreateChangeTx() {

    ODatabaseDocument db =
        new ODatabaseDocumentTx("memory:" + HookChangeValidationTest.class.getSimpleName());
    db.create();
    try {
      OSchema schema = db.getMetadata().getSchema();
      OClass classA = schema.createClass("TestClass");
      classA.createProperty("property1", OType.STRING).setNotNull(true);
      classA.createProperty("property2", OType.STRING).setReadonly(true);
      classA.createProperty("property3", OType.STRING).setMandatory(true);
      db.registerHook(
          new ODocumentHookAbstract() {
            @Override
            public RESULT onRecordBeforeCreate(ODocument doc) {
              doc.removeField("property1");
              doc.removeField("property2");
              doc.removeField("property3");
              return RESULT.RECORD_CHANGED;
            }

            @Override
            public RESULT onRecordBeforeUpdate(ODocument doc) {
              return RESULT.RECORD_NOT_CHANGED;
            }

            @Override
            public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
              return DISTRIBUTED_EXECUTION_MODE.SOURCE_NODE;
            }
          });
      ODocument doc = new ODocument(classA);
      doc.field("property1", "value1-create");
      doc.field("property2", "value2-create");
      doc.field("property3", "value3-create");
      try {
        db.begin();
        doc.save();
        db.commit();
        Assert.fail("The document save should fail for validation exception");
      } catch (OValidationException ex) {

      }
    } finally {
      db.drop();
    }
  }

  @Test
  public void testHookUpdateChangeTx() {

    ODatabaseDocument db =
        new ODatabaseDocumentTx("memory:" + HookChangeValidationTest.class.getSimpleName());
    db.create();
    try {
      OSchema schema = db.getMetadata().getSchema();
      OClass classA = schema.createClass("TestClass");
      classA.createProperty("property1", OType.STRING).setNotNull(true);
      classA.createProperty("property2", OType.STRING).setReadonly(true);
      classA.createProperty("property3", OType.STRING).setMandatory(true);
      db.registerHook(
          new ODocumentHookAbstract() {
            @Override
            public RESULT onRecordBeforeCreate(ODocument doc) {
              return RESULT.RECORD_NOT_CHANGED;
            }

            @Override
            public RESULT onRecordBeforeUpdate(ODocument doc) {
              doc.removeField("property1");
              doc.removeField("property2");
              doc.removeField("property3");
              return RESULT.RECORD_CHANGED;
            }

            @Override
            public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
              return DISTRIBUTED_EXECUTION_MODE.SOURCE_NODE;
            }
          });
      ODocument doc = new ODocument(classA);
      doc.field("property1", "value1-create");
      doc.field("property2", "value2-create");
      doc.field("property3", "value3-create");
      doc.save();
      assertEquals("value1-create", doc.field("property1"));
      assertEquals("value2-create", doc.field("property2"));
      assertEquals("value3-create", doc.field("property3"));

      doc.field("property1", "value1-update");
      doc.field("property2", "value2-update");
      try {
        db.begin();
        doc.save();
        db.commit();
        Assert.fail("The document save should fail for validation exception");
      } catch (OValidationException ex) {

      }
    } finally {
      db.drop();
    }
  }
}
