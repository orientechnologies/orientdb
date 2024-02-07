package com.orientechnologies.lucene.test;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.io.InputStream;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.Test;

/** Created by frank on 27/04/2017. */
public class LuceneIssuesTest extends BaseLuceneTest {
  @Test
  public void testGh_7382() throws Exception {

    try (InputStream stream = ClassLoader.getSystemResourceAsStream("testGh_7382.osql")) {
      db.execute("sql", getScriptFromStream(stream)).close();
    }

    final OIndex index =
        db.getMetadata().getIndexManagerInternal().getIndex(db, "class_7382_multi");
    try (Stream<ORID> rids =
        index
            .getInternal()
            .getRids("server:206012226875414 AND date:[201703120000 TO  201703120001]")) {
      Assertions.assertThat(rids.count()).isEqualTo(1);
    }
  }

  @Test
  public void testGh_4880_moreIndexesOnProperty() throws Exception {
    try (final InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql")) {
      db.execute("sql", getScriptFromStream(stream)).close();
    }

    db.command("create index Song.title_ft on Song (title,author) FULLTEXT ENGINE LUCENE").close();
    db.command("CREATE INDEX Song.author on Song (author)  NOTUNIQUE").close();

    db.query("SELECT from Song where title = 'BELIEVE IT OR NOT' ").close();

    db.command(
            "EXPLAIN SELECT from Song where author = 'Traditional'  OR [title,author] LUCENE"
                + " '(title:believe'")
        .close();
  }

  @Test
  public void testGh_issue7513() throws Exception {

    try (InputStream stream = ClassLoader.getSystemResourceAsStream("testGh_7513.osql")) {
      db.execute("sql", getScriptFromStream(stream)).close();
    }

    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "Item.content");
    try (Stream<ORID> rids = index.getInternal().getRids("'Харько~0.2")) {
      Assertions.assertThat(rids.count() >= 3).isTrue();
    }
  }

  @Test
  public void test_ph8929() throws Exception {
    try (InputStream stream = ClassLoader.getSystemResourceAsStream("testPh_8929.osql")) {
      db.execute("sql", getScriptFromStream(stream)).close();
    }

    OResultSet documents;

    documents = db.query("select from Test where [a] lucene 'lion'");

    Assertions.assertThat(documents).hasSize(1);

    documents = db.query("select from Test where [b] lucene 'mouse'");

    Assertions.assertThat(documents).hasSize(1);

    documents = db.query("select from Test where [a] lucene 'lion' OR [b] LUCENE 'mouse' ");

    Assertions.assertThat(documents).hasSize(2);
  }

  @Test
  public void test_ph8929_Single() throws Exception {

    try (InputStream stream = ClassLoader.getSystemResourceAsStream("testPh_8929.osql")) {
      db.execute("sql", getScriptFromStream(stream)).close();
    }

    OResultSet documents;

    documents = db.query("select from Test where a lucene 'lion'");

    Assertions.assertThat(documents).hasSize(1);

    documents = db.query("select from Test where b lucene 'mouse'");

    Assertions.assertThat(documents).hasSize(1);

    documents = db.query("select from Test where a lucene 'lion' OR b LUCENE 'mouse' ");

    Assertions.assertThat(documents).hasSize(2);
  }
}
