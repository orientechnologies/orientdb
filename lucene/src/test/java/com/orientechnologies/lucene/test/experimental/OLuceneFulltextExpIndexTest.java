package com.orientechnologies.lucene.test.experimental;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.lucene.test.BaseLuceneTest;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.logging.Level;

/**
 * Created by frank on 9/30/15.
 */
@Test(enabled = false)
public class OLuceneFulltextExpIndexTest extends BaseLuceneTest {

  public OLuceneFulltextExpIndexTest() {
    //    super(false);
  }

  @Override
  protected String getDatabaseName() {
    return getClass().getSimpleName();
  }

  @BeforeClass
  public void init() throws IOException {
    OLogManager.instance().installCustomFormatter();
    OLogManager.instance().setConsoleLevel(Level.INFO.getName());

    initDB();

    databaseDocumentTx.command(new OCommandSQL("ALTER DATABASE custom strictSql=false")).execute();

    OSchema schema = databaseDocumentTx.getMetadata().getSchema();
    OClass v = schema.getClass("V");
    OClass song = schema.createClass("Song");
    song.setSuperClass(v);
    song.createProperty("title", OType.STRING);
    song.createProperty("author", OType.STRING);
    song.createProperty("lyrics", OType.STRING);

    databaseDocumentTx.setProperty("CUSTOM", "strictSql=false");
    databaseDocumentTx.command(new OCommandSQL(
        "create index Song.all on Song (title,author,lyrics) FULLTEXTEXP ENGINE LUCENEEXP METADATA {"
            + "\"title_index_analyzer\":\""            + StandardAnalyzer.class.getName() + "\" , "
            + "\"author_index_analyzer\":\"" + StandardAnalyzer.class.getName() + "\" , "
            + "\"lyrics_index_analyzer\":\"" + EnglishAnalyzer.class.getName()
            + "\"}")).execute();

    //     databaseDocumentTx.command(
    //     new OCommandSQL("create index Song.title on Song (title) FULLTEXTEXP ENGINE LUCENE METADATA {\"index_analyzer\":\""
    //     + StandardAnalyzer.class.getName() + "\"}")).execute();
    //
    // databaseDocumentTx.command(
    // new OCommandSQL("create index Song.author on Song (author) FULLTEXTEXP ENGINE LUCENE METADATA {\"index_analyzer\":\""
    // + StandardAnalyzer.class.getName() + "\"}")).execute();
    //
    // databaseDocumentTx.command(
    // new OCommandSQL("create index Song.lyrics on Song (lyrics) FULLTEXTEXP ENGINE LUCENE METADATA {\"index_analyzer\":\""
    // + EnglishAnalyzer.class.getName() + "\"}")).execute();

    String fromStream = OIOUtils.readFileAsString(new File("./src/test/resources/testLuceneIndex.sql"), StandardCharsets.UTF_8);
    databaseDocumentTx.command(new OCommandScript("sql", fromStream)).execute();

  }

  @AfterClass(enabled = true)
  public void after() {

    //    deInitDB();
  }

  @Test(enabled = false)
  public void testName() throws Exception {

    final ODocument index = databaseDocumentTx.getMetadata().getIndexManager().getIndex("Song.all").getMetadata();

    Assert.assertEquals(index.field("lyrics_index_analyzer"), EnglishAnalyzer.class.getName());
    Assert.assertEquals(index.field("author_index_analyzer"), StandardAnalyzer.class.getName());
    Assert.assertEquals(index.field("title_index_analyzer"), StandardAnalyzer.class.getName());
    List<ODocument> docs;

    docs = databaseDocumentTx
        .query(new OSQLSynchQuery<ODocument>("select * from Song where [title] LUCENEEXP \"Song.title:mountain\""));

    Assert.assertEquals(docs.size(), 4);

    docs = databaseDocumentTx
        .query(new OSQLSynchQuery<ODocument>("select * from Song where [author] LUCENEEXP \"Song.author:Fabbio\""));

    Assert.assertEquals(docs.size(), 87);
    //
    //    // not WORK BECAUSE IT USES only the first index
    //    // String query = "select * from Song where [title] LUCENE \"(title:mountain)\"  and [author] LUCENE \"(author:Fabbio)\""
    //    String query = "select * from Song where [title] LUCENE_EXP (title:mountain)  and author = 'Fabbio'";
    //    docs = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>(query));
    //
    //    Assert.assertEquals(docs.size(), 1);

  }

}
