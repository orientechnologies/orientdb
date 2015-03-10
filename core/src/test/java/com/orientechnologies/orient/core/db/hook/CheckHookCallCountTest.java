package com.orientechnologies.orient.core.db.hook;

import static org.testng.AssertJUnit.assertEquals;

import java.util.List;
import java.util.UUID;

import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

public class CheckHookCallCountTest {
  private final String CLASS_NAME   = "Data";
  private final String FIELD_ID     = "ID";
  private final String FIELD_STATUS = "STATUS";
  private final String STATUS       = "processed";

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

  @Test
  public void testMultipleCallHook() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:temp");
    db.create();
    try {
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

      System.out.println("WITHOUT INDEX: onRecordAfterRead will be called twice");
      db.query(new OSQLSynchQuery<ODocument>("SELECT FROM " + CLASS_NAME + " WHERE " + FIELD_STATUS + " = '" + STATUS + "'"));
      assertEquals(hook.readCount, 1);
      hook.readCount = 0;
      System.out.println("WITH INDEX: onRecordAfterRead will be called only once");
      db.query(new OSQLSynchQuery<ODocument>("SELECT FROM " + CLASS_NAME + " WHERE " + FIELD_ID + " = '" + id + "'"));
      assertEquals(hook.readCount, 1);
    } finally {
      db.drop();
    }
  }

}
