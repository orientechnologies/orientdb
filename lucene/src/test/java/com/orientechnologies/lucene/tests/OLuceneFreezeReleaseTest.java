package com.orientechnologies.lucene.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Before;
import org.junit.Test;

/** Created by Enrico Risa on 23/09/16. */
public class OLuceneFreezeReleaseTest extends OLuceneBaseTest {

  @Before
  public void setUp() throws Exception {

    dropDatabase();
    super.setupDatabase("ci");
  }

  @Test
  public void freezeReleaseTest() {

    OSchema schema = db.getMetadata().getSchema();
    OClass person = schema.createClass("Person");
    person.createProperty("name", OType.STRING);

    db.command("create index Person.name on Person (name) FULLTEXT ENGINE LUCENE");

    db.save(new ODocument("Person").field("name", "John"));

    OResultSet results = db.query("select from Person where search_class('John')=true");

    assertThat(results).hasSize(1);
    results.close();

    db.freeze();

    results = db.command("select from Person where search_class('John')=true");
    assertThat(results).hasSize(1);
    results.close();

    db.release();

    ODocument doc = db.newInstance("Person");
    doc.field("name", "John");
    db.save(doc);

    results = db.query("select from Person where search_class('John')=true");
    assertThat(results).hasSize(2);
    results.close();
  }

  // With double calling freeze/release
  @Test
  public void freezeReleaseMisUsageTest() {

    OSchema schema = db.getMetadata().getSchema();
    OClass person = schema.createClass("Person");
    person.createProperty("name", OType.STRING);

    db.command("create index Person.name on Person (name) FULLTEXT ENGINE LUCENE");

    db.save(new ODocument("Person").field("name", "John"));

    OResultSet results = db.command("select from Person where search_class('John')=true");

    assertThat(results).hasSize(1);
    results.close();

    db.freeze();

    db.freeze();

    results = db.command("select from Person where search_class('John')=true");

    assertThat(results).hasSize(1);
    results.close();

    db.release();
    db.release();

    db.save(new ODocument("Person").field("name", "John"));

    results = db.command("select from Person where search_class('John')=true");
    assertThat(results).hasSize(2);
    results.close();
  }
}
