package com.orientechnologies.lucene.tests;

import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by Enrico Risa on 05/10/16.
 */
public class OLuceneNullTest extends OLuceneBaseTest {

  @Before
  public void setUp() throws Exception {
    db.command("create class Test extends V");

    db.command("create property Test.names EMBEDDEDLIST STRING");

    db.command("create index Test.names on Test(names) FULLTEXT ENGINE LUCENE");


  }

  @Test
  public void testNullChangeToNotNullWithLists() {


    db.begin();
    ODocument doc = new ODocument("Test");
    db.save(doc);
    db.commit();

    db.begin();
    doc.field("names", new String[] { "foo" });
    db.save(doc);
    db.commit();

    OIndex<?> index = db.getMetadata().getIndexManager().getIndex("Test.names");

    Assert.assertEquals(index.getSize(), 1);

  }

  @Test
  public void testNotNullChangeToNullWithLists() {


    ODocument doc = new ODocument("Test");

    db.begin();
    doc.field("names", new String[] { "foo" });
    db.save(doc);
    db.commit();

    db.begin();

    doc.removeField("names");

    db.save(doc);
    db.commit();

    OIndex<?> index = db.getMetadata().getIndexManager().getIndex("Test.names");
    Assert.assertEquals(index.getSize(), 0);

  }
}