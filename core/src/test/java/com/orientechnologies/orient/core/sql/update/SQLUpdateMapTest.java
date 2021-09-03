package com.orientechnologies.orient.core.sql.update;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinary;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import java.util.Map;
import org.junit.Test;

public class SQLUpdateMapTest {

  @Test
  public void testMapPut() {
    ODatabaseDocument db =
        new ODatabaseDocumentTx("memory:" + SQLUpdateMapTest.class.getSimpleName());
    try {
      ((ODatabaseDocumentTx) db).setSerializer(new ORecordSerializerBinary());
      db.create();
      db.command(new OCommandSQL("create class vRecord")).execute();
      ODocument ret =
          db.command(new OCommandSQL("insert into vRecord (title) values('first record')"))
              .execute();
      ODocument ret1 =
          db.command(new OCommandSQL("insert into vRecord (title) values('second record')"))
              .execute();

      db.command(
              new OCommandSQL(
                  "update " + ret.getIdentity() + " put attrs = 'test1', " + ret1.getIdentity()))
          .execute();
      db.close();
      db.open("admin", "admin");
      db.getLocalCache().clear();
      db.command(
              new OCommandSQL("update " + ret.getIdentity() + " put attrs = 'test', 'test value' "))
          .execute();
      ret.reload();
      assertEquals(2, ((Map) ret.field("attrs")).size());
      assertEquals("test value", ((Map) ret.field("attrs")).get("test"));
      assertEquals(ret1.getIdentity(), ((Map) ret.field("attrs")).get("test1"));
    } finally {
      db.drop();
    }
  }
}
