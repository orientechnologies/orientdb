package com.orientechnologies.lucene.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Date;
import org.junit.Test;

/** Created by Enrico Risa on 27/10/16. */
public class OLuceneReuseTest extends OLuceneBaseTest {

  @Test
  public void shouldUseTheRightIndex() {

    OSchema schema = db.getMetadata().getSchema();

    OClass cls = schema.createClass("Reuse");
    cls.createProperty("name", OType.STRING);
    cls.createProperty("date", OType.DATETIME);
    cls.createProperty("surname", OType.STRING);
    cls.createProperty("age", OType.LONG);

    db.command("create index Reuse.composite on Reuse (name,surname,date,age) UNIQUE");
    db.command("create index Reuse.surname on Reuse (surname) FULLTEXT ENGINE LUCENE");

    for (int i = 0; i < 10; i++) {
      db.save(
          new ODocument("Reuse")
              .field("name", "John")
              .field("date", new Date())
              .field("surname", "Reese")
              .field("age", i));
    }
    OResultSet results =
        db.command("SELECT FROM Reuse WHERE name='John' and search_class('Reese') =true");

    assertThat(results).hasSize(10);

    results = db.command("SELECT FROM Reuse WHERE search_class('Reese')=true  and name='John'");

    assertThat(results).hasSize(10);
  }

  @Test
  public void shouldUseTheRightLuceneIndex() {

    OSchema schema = db.getMetadata().getSchema();

    OClass cls = schema.createClass("Reuse");
    cls.createProperty("name", OType.STRING);
    cls.createProperty("date", OType.DATETIME);
    cls.createProperty("surname", OType.STRING);
    cls.createProperty("age", OType.LONG);

    db.command("create index Reuse.composite on Reuse (name,surname,date,age) UNIQUE");

    // lucene on name and surname
    db.command("create index Reuse.name_surname on Reuse (name,surname) FULLTEXT ENGINE LUCENE");

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

    // exact query on name uses Reuse.conposite
    OResultSet results =
        db.command("SELECT FROM Reuse WHERE name='John' and search_class('Reese')=true");

    assertThat(results).hasSize(10);

    results = db.command("SELECT FROM Reuse WHERE search_class('Reese')=true and name='John'");

    assertThat(results).hasSize(10);

    results =
        db.command(
            "SELECT FROM Reuse WHERE name='John' AND search_class('surname:Franklin') =true");

    assertThat(results).hasSize(1);
  }
}
