package com.orientechnologies.lucene.test;

import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.io.InputStream;
import java.util.List;

/**
 * Created by frank on 27/04/2017.
 */
public class LuceneIssuesTest extends BaseLuceneTest {

  @Test
  public void testGh_7382() throws Exception {

    InputStream stream = ClassLoader.getSystemResourceAsStream("testGh_7382.osql");

    db.command(new OCommandScript("sql", getScriptFromStream(stream))).execute();

    List<ODocument> results = db.query(new OSQLSynchQuery<ODocument>(
        "select server,date from index:class_7382_multi WHERE key = 'server:206012226875414 AND date:[201703120000 TO  201703120001]' "));

    Assertions.assertThat(results).hasSize(1);

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
  public void testGh_issue7513() throws Exception {

    InputStream stream = ClassLoader.getSystemResourceAsStream("testGh_7513.osql");

    db.command(new OCommandScript("sql", getScriptFromStream(stream))).execute();

    List<ODocument> documents = db
        .query(new OSQLSynchQuery<Object>("select rid from index:Item.content where key lucene 'Харько~0.2' limit 3 "));

    Assertions.assertThat(documents).hasSize(3);
    documents = db
        .query(new OSQLSynchQuery<Object>("select expand(rid) from index:Item.content where key lucene 'Харько~0.2' limit 3 "));

    Assertions.assertThat(documents).hasSize(3);
    documents = db.query(new OSQLSynchQuery<Object>("select * from index:Item.content where key lucene 'Харько~0.2' limit 3 "));

    Assertions.assertThat(documents).hasSize(3);

  }

  @Test
  public void test_ph8929() throws Exception {

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
