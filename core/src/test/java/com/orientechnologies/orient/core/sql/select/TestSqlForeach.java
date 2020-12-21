package com.orientechnologies.orient.core.sql.select;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class TestSqlForeach {

  @Test
  public void testForeach() {
    ODatabaseDocument db =
        new ODatabaseDocumentTx("memory:" + TestSqlForeach.class.getSimpleName());
    db.create();
    db.getMetadata().getSchema().createClass("Test");
    ODocument doc = new ODocument("Test");
    db.save(doc);

    OResultSet result =
        db.execute(
            "sql",
            "let $res = select from Test; foreach ($r in $res) { update $r set timestamp = sysdate(); }; return $res; ");

    Assert.assertTrue(result.hasNext());

    while (result.hasNext()) {
      result.next();
    }
  }
}
