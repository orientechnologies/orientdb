package com.orientechnologies.orient.core.db.hook;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.UUID;
import org.junit.Test;

public class CheckHookCallCountTest extends BaseMemoryDatabase {
  private final String CLASS_NAME = "Data";
  private final String FIELD_ID = "ID";
  private final String FIELD_STATUS = "STATUS";
  private final String STATUS = "processed";

  @Test
  public void testMultipleCallHook() {
    OClass aClass = db.getMetadata().getSchema().createClass(CLASS_NAME);
    aClass.createProperty(FIELD_ID, OType.STRING);
    aClass.createProperty(FIELD_STATUS, OType.STRING);
    aClass.createIndex("IDX", OClass.INDEX_TYPE.NOTUNIQUE, FIELD_ID);
    TestHook hook = new TestHook();
    db.registerHook(hook);

    String id = UUID.randomUUID().toString();
    ODocument first = new ODocument(CLASS_NAME);
    first.field(FIELD_ID, id);
    first.field(FIELD_STATUS, STATUS);
    db.save(first);

    db.query("SELECT FROM " + CLASS_NAME + " WHERE " + FIELD_STATUS + " = '" + STATUS + "'")
        .stream()
        .count();
    //      assertEquals(hook.readCount, 1); //TODO
    hook.readCount = 0;
    db.query("SELECT FROM " + CLASS_NAME + " WHERE " + FIELD_ID + " = '" + id + "'").stream()
        .count();
    //      assertEquals(hook.readCount, 1); //TODO
  }

  @Test
  public void testInHook() throws Exception {
    OSchema schema = db.getMetadata().getSchema();
    OClass oClass = schema.createClass("TestInHook");
    oClass.createProperty("a", OType.INTEGER);
    oClass.createProperty("b", OType.INTEGER);
    oClass.createProperty("c", OType.INTEGER);
    ODocument doc = new ODocument(oClass);
    doc.field("a", 2);
    doc.field("b", 2);
    doc.save();
    doc.reload();
    assertEquals(Integer.valueOf(2), doc.field("a"));
    assertEquals(Integer.valueOf(2), doc.field("b"));
    assertNull(doc.field("c"));
    db.registerHook(
        new ODocumentHookAbstract(db) {

          {
            setIncludeClasses("TestInHook");
          }

          @Override
          public void onRecordAfterCreate(ODocument iDocument) {
            onRecordAfterRead(iDocument);
          }

          @Override
          public void onRecordAfterRead(ODocument iDocument) {
            String script = "select sum(a, b) as value from " + iDocument.getIdentity();
            try (OResultSet calculated = database.query(script)) {
              if (calculated.hasNext()) {
                iDocument.field("c", calculated.next().<Object>getProperty("value"));
              }
            }
          }

          @Override
          public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
            return DISTRIBUTED_EXECUTION_MODE.SOURCE_NODE;
          }
        });
    doc.reload();
    assertEquals(Integer.valueOf(2), doc.field("a"));
    assertEquals(Integer.valueOf(2), doc.field("b"));
    assertEquals(Integer.valueOf(4), doc.field("c"));

    doc = new ODocument(oClass);
    doc.field("a", 3);
    doc.field("b", 3);
    doc.save(); // FAILING here: infinite recursion

    assertEquals(Integer.valueOf(3), doc.field("a"));
    assertEquals(Integer.valueOf(3), doc.field("b"));
    assertEquals(Integer.valueOf(6), doc.field("c"));
  }

  public class TestHook extends ODocumentHookAbstract {
    public int readCount;

    @Override
    public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
      return DISTRIBUTED_EXECUTION_MODE.BOTH;
    }

    @Override
    public void onRecordAfterRead(ODocument iDocument) {
      readCount++;
    }
  }
}
