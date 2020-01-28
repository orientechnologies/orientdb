package com.orientechnologies.lucene.test;

import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.io.InputStream;
import java.util.List;
import java.util.stream.Stream;

/**
 * Created by frank on 27/04/2017.
 */
public class LuceneIssuesTest extends BaseLuceneTest {

  @Test
  public void testGh_7382() {

    InputStream stream = ClassLoader.getSystemResourceAsStream("testGh_7382.osql");

    db.command(new OCommandScript("sql", getScriptFromStream(stream))).execute();

    final OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "class_7382_multi");
    try (Stream<ORID> rids = index.getInternal().getRids("server:206012226875414 AND date:[201703120000 TO  201703120001]")) {
      Assertions.assertThat(rids.count()).isEqualTo(1);
    }
  }

  @Test
  public void testGh_4880_moreIndexesOnProperty() throws Exception {

    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");

    db.command(new OCommandScript("sql", getScriptFromStream(stream))).execute();

    db.command(new OCommandSQL("create index Song.title_ft on Song (title,author) FULLTEXT ENGINE LUCENE")).execute();
    db.command(new OCommandSQL("CREATE INDEX Song.author on Song (author)  NOTUNIQUE")).execute();

    db.query(new OSQLSynchQuery<Object>("SELECT from Song where title = 'BELIEVE IT OR NOT' "));

    ODocument query = db.command(
        new OCommandSQL("EXPLAIN SELECT from Song where author = 'Traditional'  OR ['title','author'] LUCENE 'title:believe'"))
        .execute();

  }

  @Test
  public void testGh_issue7513() {

    InputStream stream = ClassLoader.getSystemResourceAsStream("testGh_7513.osql");

    db.command(new OCommandScript("sql", getScriptFromStream(stream))).execute();

    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "Item.content");
    try (Stream<ORID> rids = index.getInternal().getRids("'Харько~0.2")) {
      Assertions.assertThat(rids.count() >= 3).isTrue();
    }
  }

  @Test
  public void test_ph8929() {

    InputStream stream = ClassLoader.getSystemResourceAsStream("testPh_8929.osql");

    db.command(new OCommandScript("sql", getScriptFromStream(stream))).execute();

    List<ODocument> documents;

    documents = db.query(new OSQLSynchQuery<Object>("select from Test where [a] lucene 'lion'"));

    Assertions.assertThat(documents).hasSize(1);

    documents = db.query(new OSQLSynchQuery<Object>("select from Test where [b] lucene 'mouse'"));

    Assertions.assertThat(documents).hasSize(1);

    documents = db.query(new OSQLSynchQuery<Object>("select from Test where [a] lucene 'lion' OR [b] LUCENE 'mouse' "));

    //FIXME
    Assertions.assertThat(documents).hasSize(2);

  }

  @Test
  public void test_ph8929_Single() throws Exception {

    InputStream stream = ClassLoader.getSystemResourceAsStream("testPh_8929.osql");

    db.command(new OCommandScript("sql", getScriptFromStream(stream))).execute();

    List<ODocument> documents;

    documents = db.query(new OSQLSynchQuery<Object>("select from Test where a lucene 'lion'"));

    Assertions.assertThat(documents).hasSize(1);

    documents = db.query(new OSQLSynchQuery<Object>("select from Test where b lucene 'mouse'"));

    Assertions.assertThat(documents).hasSize(1);

    documents = db.query(new OSQLSynchQuery<Object>("select from Test where a lucene 'lion' OR b LUCENE 'mouse' "));

    //FIXME
    Assertions.assertThat(documents).hasSize(2);

  }
}
