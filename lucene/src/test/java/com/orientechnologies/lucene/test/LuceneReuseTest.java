package com.orientechnologies.lucene.test;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Date;
import org.junit.Assert;
import org.junit.Test;

/** Created by Enrico Risa on 27/10/16. */
public class LuceneReuseTest extends BaseLuceneTest {

  @Test
  public void shouldUseTheRightIndex() {

    OSchema schema = db.getMetadata().getSchema();

    OClass cls = schema.createClass("Reuse");
    cls.createProperty("name", OType.STRING);
    cls.createProperty("date", OType.DATETIME);
    cls.createProperty("surname", OType.STRING);
    cls.createProperty("age", OType.LONG);

    db.command("create index Reuse.composite on Reuse (name,surname,date,age) UNIQUE").close();
    db.command("create index Reuse.surname on Reuse (surname) FULLTEXT ENGINE LUCENE").close();

    for (int i = 0; i < 10; i++) {
      db.save(
          new ODocument("Reuse")
              .field("name", "John")
              .field("date", new Date())
              .field("surname", "Reese")
              .field("age", i));
    }
    OResultSet results =
        db.command("SELECT FROM Reuse WHERE name='John' and surname LUCENE 'Reese'");

    Assert.assertEquals(10, results.stream().count());

    results = db.command("SELECT FROM Reuse WHERE surname LUCENE 'Reese' and name='John'");

    Assert.assertEquals(10, results.stream().count());
  }

  @Test
  public void shouldUseTheRightLuceneIndex() {

    OSchema schema = db.getMetadata().getSchema();

    OClass cls = schema.createClass("Reuse");
    cls.createProperty("name", OType.STRING);
    cls.createProperty("date", OType.DATETIME);
    cls.createProperty("surname", OType.STRING);
    cls.createProperty("age", OType.LONG);

    db.command("create index Reuse.composite on Reuse (name,surname,date,age) UNIQUE").close();

    // lucene on name and surname
    db.command("create index Reuse.name_surname on Reuse (name,surname) FULLTEXT ENGINE LUCENE")
        .close();

    for (int i = 0; i < 10; i++) {
      db.save(
          new ODocument("Reuse")
              .field("name", "John")
              .field("date", new Date())
              .field("surname", "Reese")
              .field("age", i));
    }

    // additional record
    db.save(
        new ODocument("Reuse")
            .field("name", "John")
            .field("date", new Date())
            .field("surname", "Franklin")
            .field("age", 11));
    OResultSet results =
        db.command("SELECT FROM Reuse WHERE name='John' and [name,surname] LUCENE 'Reese'");

    Assert.assertEquals(10, results.stream().count());

    results = db.command("SELECT FROM Reuse WHERE [name,surname] LUCENE 'Reese' and name='John'");

    Assert.assertEquals(10, results.stream().count());

    results =
        db.command(
            "SELECT FROM Reuse WHERE name='John' and [name,surname] LUCENE '(surname:Franklin)'");

    Assert.assertEquals(1, results.stream().count());
  }
}
