package com.orientechnologies.lucene.test;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import java.io.File;
import java.util.Collection;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Created by Enrico Risa on 23/09/16. */
public class LuceneFreezeReleaseTest {

  @Before
  public void setUp() throws Exception {
    OFileUtils.deleteRecursively(new File("./target/freezeRelease"));
  }

  @Test
  public void freezeReleaseTest() {
    if (isWindows()) return;

    ODatabaseDocument db = new ODatabaseDocumentTx("plocal:target/freezeRelease");

    db.create();

    OSchema schema = db.getMetadata().getSchema();
    OClass person = schema.createClass("Person");
    person.createProperty("name", OType.STRING);

    db.command(new OCommandSQL("create index Person.name on Person (name) FULLTEXT ENGINE LUCENE"))
        .execute();

    db.save(new ODocument("Person").field("name", "John"));

    try {

      Collection results =
          db.command(new OCommandSQL("select from Person where name lucene 'John'")).execute();
      Assert.assertEquals(1, results.size());
      db.freeze();

      results =
          db.command(new OCommandSQL("select from Person where name lucene 'John'")).execute();
      Assert.assertEquals(1, results.size());

      db.release();

      db.save(new ODocument("Person").field("name", "John"));

      results =
          db.command(new OCommandSQL("select from Person where name lucene 'John'")).execute();
      Assert.assertEquals(2, results.size());

    } finally {

      db.drop();
    }
  }

  // With double calling freeze/release
  @Test
  public void freezeReleaseMisUsageTest() {
    if (isWindows()) return;

    ODatabaseDocument db = new ODatabaseDocumentTx("plocal:target/freezeRelease");

    db.create();

    OSchema schema = db.getMetadata().getSchema();
    OClass person = schema.createClass("Person");
    person.createProperty("name", OType.STRING);

    db.command(new OCommandSQL("create index Person.name on Person (name) FULLTEXT ENGINE LUCENE"))
        .execute();

    db.save(new ODocument("Person").field("name", "John"));

    try {

      Collection results =
          db.command(new OCommandSQL("select from Person where name lucene 'John'")).execute();
      Assert.assertEquals(1, results.size());

      db.freeze();

      db.freeze();

      results =
          db.command(new OCommandSQL("select from Person where name lucene 'John'")).execute();
      Assert.assertEquals(1, results.size());

      db.release();
      db.release();

      db.save(new ODocument("Person").field("name", "John"));

      results =
          db.command(new OCommandSQL("select from Person where name lucene 'John'")).execute();
      Assert.assertEquals(2, results.size());

    } finally {
      db.drop();
    }
  }

  private boolean isWindows() {
    final String osName = System.getProperty("os.name").toLowerCase();
    return osName.contains("win");
  }
}
