package com.orientechnologies.orient.core.sql.select;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Assert;
import org.junit.Test;

public class TestSqlForeach extends BaseMemoryDatabase {

  @Test
  public void testForeach() {
    db.getMetadata().getSchema().createClass("Test");
    ODocument doc = new ODocument("Test");
    db.save(doc);

    OResultSet result =
        db.execute(
            "sql",
            "let $res = select from Test; foreach ($r in $res) { update $r set timestamp ="
                + " sysdate(); }; return $res; ");

    Assert.assertTrue(result.hasNext());

    while (result.hasNext()) {
      result.next();
    }
  }
}
