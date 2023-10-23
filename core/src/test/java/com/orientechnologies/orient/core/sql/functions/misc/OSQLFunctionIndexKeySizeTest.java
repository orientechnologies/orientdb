package com.orientechnologies.orient.core.sql.functions.misc;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Assert;
import org.junit.Test;

public class OSQLFunctionIndexKeySizeTest extends BaseMemoryDatabase {

  @Test
  public void test() {
    OClass clazz = db.getMetadata().getSchema().createClass("Test");
    clazz.createProperty("name", OType.STRING);
    db.command("create index testindex on  Test (name) notunique").close();
    db.command("insert into Test set name = 'a'").close();
    db.command("insert into Test set name = 'b'").close();

    try (OResultSet rs = db.query("select indexKeySize('testindex') as foo")) {
      Assert.assertTrue(rs.hasNext());
      OResult item = rs.next();
      Assert.assertEquals((Object) 2L, item.getProperty("foo"));
      Assert.assertFalse(rs.hasNext());
    }
  }
}
