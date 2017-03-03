package com.orientechnologies.lucene.tests;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by Enrico Risa on 23/09/16.
 */
public class OLuceneFreezeReleaseTest {

  @Test
  public void freezeReleaseTest() {

    ODatabaseDocument db = new ODatabaseDocumentTx("plocal:target/freezeRelease");

    db.create();

    OSchema schema = db.getMetadata().getSchema();
    OClass person = schema.createClass("Person");
    person.createProperty("name", OType.STRING);

    db.command("create index Person.name on Person (name) FULLTEXT ENGINE LUCENE");

    db.save(new ODocument("Person").field("name", "John"));

    try {

      OResultSet results = db.command("select from Person where search_class('John')=true");

      assertThat(results).hasSize(1);
      results.close();

      db.freeze();

      results = db.command("select from Person where search_class('John')=true");
      assertThat(results).hasSize(1);
      results.close();

      db.release();

      db.save(new ODocument("Person").field("name", "John"));

      results = db.command("select from Person where search_class('John')=true");
      assertThat(results).hasSize(2);
      results.close();

    } finally {

      db.drop();
    }

  }

  // With double calling freeze/release
  @Test
  public void freezeReleaseMisUsageTest() {

    ODatabaseDocument db = new ODatabaseDocumentTx("plocal:target/freezeRelease");

    db.create();

    OSchema schema = db.getMetadata().getSchema();
    OClass person = schema.createClass("Person");
    person.createProperty("name", OType.STRING);

    db.command("create index Person.name on Person (name) FULLTEXT ENGINE LUCENE");

    db.save(new ODocument("Person").field("name", "John"));

    try {

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

    } finally {

      db.drop();
    }

  }
}
