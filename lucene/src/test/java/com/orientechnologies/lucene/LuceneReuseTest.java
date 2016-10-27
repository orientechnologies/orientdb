package com.orientechnologies.lucene;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.Date;

/**
 * Created by Enrico Risa on 27/10/16.
 */
public class LuceneReuseTest {

  @Test
  public void shouldUseTheRightIndex() {

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:luceneTest");

    db.create();

    try {
      OSchema schema = db.getMetadata().getSchema();

      OClass cls = schema.createClass("Reuse");
      cls.createProperty("name", OType.STRING);
      cls.createProperty("date", OType.DATETIME);
      cls.createProperty("surname", OType.STRING);
      cls.createProperty("age", OType.LONG);

      db.command(new OCommandSQL("create index Reuse.comp on Reuse (name,surname,date,age) UNIQUE")).execute();
      db.command(new OCommandSQL("create index Reuse.surname on Reuse (surname) FULLTEXT ENGINE LUCENE")).execute();

      for (int i = 0; i < 10; i++) {
        db.save(new ODocument("Reuse").field("name", "John").field("date", new Date()).field("surname", "Reese").field("age", i));
      }
      Collection<ODocument> results = db.command(new OCommandSQL("SELECT FROM Reuse WHERE name='John' and surname LUCENE 'Reese'"))
          .execute();

      Assert.assertEquals(10, results.size());

      results = db.command(new OCommandSQL("SELECT FROM Reuse WHERE surname LUCENE 'Reese' and name='John'")).execute();

      Assert.assertEquals(10, results.size());
    } finally {
      db.drop();
    }

  }

  @Test
  public void shouldUseTheRightLuceneIndex() {

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:luceneTest");

    db.create();

    try {
      OSchema schema = db.getMetadata().getSchema();

      OClass cls = schema.createClass("Reuse");
      cls.createProperty("name", OType.STRING);
      cls.createProperty("date", OType.DATETIME);
      cls.createProperty("surname", OType.STRING);
      cls.createProperty("age", OType.LONG);

      db.command(new OCommandSQL("create index Reuse.comp on Reuse (name,surname,date,age) UNIQUE")).execute();
      db.command(new OCommandSQL("create index Reuse.n_surname on Reuse (name,surname) FULLTEXT ENGINE LUCENE")).execute();

      for (int i = 0; i < 10; i++) {
        db.save(new ODocument("Reuse").field("name", "John").field("date", new Date()).field("surname", "Reese").field("age", i));
      }
      db.save(new ODocument("Reuse").field("name", "John").field("date", new Date()).field("surname", "Franklin").field("age", 11));
      Collection<ODocument> results = db
          .command(new OCommandSQL("SELECT FROM Reuse WHERE name='John' and [name,surname] LUCENE 'Reese'")).execute();

      Assert.assertEquals(10, results.size());

      results = db.command(new OCommandSQL("SELECT FROM Reuse WHERE [name,surname] LUCENE 'Reese' and name='John'")).execute();

      Assert.assertEquals(10, results.size());

      results = db.command(new OCommandSQL("SELECT FROM Reuse WHERE name='John' and [name,surname] LUCENE '(surname:Franklin)'"))
          .execute();

      Assert.assertEquals(1, results.size());
    } finally {
      db.drop();
    }

  }

}

