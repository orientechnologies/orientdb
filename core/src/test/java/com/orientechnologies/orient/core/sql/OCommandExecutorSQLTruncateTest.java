package com.orientechnologies.orient.core.sql;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.io.IOException;
import org.junit.Test;

public class OCommandExecutorSQLTruncateTest extends BaseMemoryDatabase {

  @Test
  public void testTruncatePlain() {
    OClass vcl = db.getMetadata().getSchema().createClass("A");
    db.getMetadata().getSchema().createClass("ab", vcl);

    ODocument doc = new ODocument("A");
    db.save(doc);
    doc = new ODocument("ab");
    db.save(doc);
    OResultSet ret = db.command("truncate class A ");

    assertEquals((long) ret.next().getProperty("count"), 1L);
  }

  @Test
  public void testTruncateAPI() throws IOException {
    OClass vcl = db.getMetadata().getSchema().createClass("A");

    ODocument doc = new ODocument("A");
    db.save(doc);
    db.getMetadata().getSchema().getClasses().stream()
        .filter(oClass -> !oClass.getName().startsWith("OSecurity")) //
        .forEach(
            oClass -> {
              if (oClass.count() > 0) {
                db.command("truncate class " + oClass.getName() + " POLYMORPHIC UNSAFE").close();
              }
            });
  }

  @Test
  public void testTruncatePolimorphic() {
    OClass vcl = db.getMetadata().getSchema().createClass("A");
    db.getMetadata().getSchema().createClass("ab", vcl);

    ODocument doc = new ODocument("A");
    db.save(doc);
    doc = new ODocument("ab");
    db.save(doc);
    try (OResultSet res = db.command("truncate class A POLYMORPHIC")) {
      assertEquals((long) res.next().getProperty("count"), 1L);
      assertEquals((long) res.next().getProperty("count"), 1L);
    }
  }
}
