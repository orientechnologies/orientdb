package com.orientechnologies.lucene.test;

import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.Assert;
import org.junit.Test;

/** Created by Enrico Risa on 05/10/16. */
public class LuceneNullTest extends BaseLuceneTest {

  @Test
  public void testNullChangeToNotNullWithLists() {

    db.command("create class Test extends V").close();

    db.command("create property Test.names EMBEDDEDLIST STRING").close();

    db.command("create index Test.names on Test (names) fulltext engine lucene").close();

    db.begin();
    ODocument doc = new ODocument("Test");
    db.save(doc);
    db.commit();

    db.begin();
    doc.field("names", new String[] {"foo"});
    db.save(doc);
    db.commit();

    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "Test.names");

    Assert.assertEquals(1, index.getInternal().size());
  }

  @Test
  public void testNotNullChangeToNullWithLists() {

    db.command("create class Test extends V").close();
    db.command("create property Test.names EMBEDDEDLIST STRING").close();
    db.command("create index Test.names on Test (names) fulltext engine lucene").close();

    ODocument doc = new ODocument("Test");

    db.begin();
    doc.field("names", new String[] {"foo"});
    db.save(doc);
    db.commit();

    db.begin();

    doc.removeField("names");

    db.save(doc);
    db.commit();

    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "Test.names");
    Assert.assertEquals(index.getInternal().size(), 0);
  }
}
