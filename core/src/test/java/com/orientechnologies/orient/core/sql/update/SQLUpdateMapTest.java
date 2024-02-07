package com.orientechnologies.orient.core.sql.update;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Map;
import org.junit.Test;

public class SQLUpdateMapTest extends BaseMemoryDatabase {

  @Test
  public void testMapPut() {

    ODocument ret;
    ODocument ret1;
    db.command("create class vRecord").close();
    db.command("create property vRecord.attrs EMBEDDEDMAP ").close();
    try (OResultSet rs = db.command("insert into vRecord (title) values('first record')")) {
      ret = (ODocument) rs.next().getRecord().get();
    }

    try (OResultSet rs = db.command("insert into vRecord (title) values('second record')")) {
      ret1 = (ODocument) rs.next().getRecord().get();
    }
    db.command(
            "update " + ret.getIdentity() + " set attrs =  {'test1':" + ret1.getIdentity() + " }")
        .close();
    reOpen("admin", "adminpwd");
    db.getLocalCache().clear();
    db.command("update " + ret.getIdentity() + " set attrs['test'] = 'test value' ").close();
    ret.reload();
    assertEquals(2, ((Map) ret.field("attrs")).size());
    assertEquals("test value", ((Map) ret.field("attrs")).get("test"));
    assertEquals(ret1.getIdentity(), ((Map) ret.field("attrs")).get("test1"));
  }
}
