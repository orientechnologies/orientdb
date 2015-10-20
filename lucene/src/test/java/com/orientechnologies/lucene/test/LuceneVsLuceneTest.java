/*
 *
 *  * Copyright 2014 Orient Technologies.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *  
 */

package com.orientechnologies.lucene.test;

import com.orientechnologies.lucene.manager.OLuceneIndexManagerAbstract;
import com.orientechnologies.lucene.utils.OLuceneIndexUtils;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

/**
 * Created by enricorisa on 08/10/14.
 */
@Test(groups = "embedded")
public class LuceneVsLuceneTest extends BaseLuceneTest {

  private IndexWriter indexWriter;

  @Override
  protected String getDatabaseName() {
    return "LuceneVsLucene";
  }

  @BeforeClass
  public void init() {
    initDB();
    OSchema schema = databaseDocumentTx.getMetadata().getSchema();
    OClass v = schema.getClass("V");
    OClass song = schema.createClass("Song");
    song.setSuperClass(v);
    song.createProperty("title", OType.STRING);
    song.createProperty("author", OType.STRING);

    try {
      Directory dir = getDirectory();
      Analyzer analyzer = new StandardAnalyzer(OLuceneIndexManagerAbstract.LUCENE_VERSION);
      IndexWriterConfig iwc = new IndexWriterConfig(OLuceneIndexManagerAbstract.LUCENE_VERSION, analyzer);
      iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
      indexWriter = new IndexWriter(dir, iwc);

    } catch (IOException e) {
      e.printStackTrace();
    }
    databaseDocumentTx.command(new OCommandSQL("create index Song.title on Song (title) FULLTEXT ENGINE LUCENE")).execute();

  }

  protected Directory getDirectory() throws IOException {
    return NIOFSDirectory.open(getPath());
  }

  private File getPath() {
    return new File(buildDirectory + "/databases/" + getDatabaseName());
  }

  @Test
  public void testLuceneVsLucene() throws IOException, ParseException {
    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");

    databaseDocumentTx.command(new OCommandScript("sql", getScriptFromStream(stream))).execute();

    for (ODocument oDocument : databaseDocumentTx.browseClass("Song")) {

      String title = oDocument.field("title");
      if (title != null) {
        Document d = new Document();
        d.add(new Field("title", title, Field.Store.NO, Field.Index.ANALYZED));

        indexWriter.addDocument(d);

      }
    }

    indexWriter.close();
    IndexReader reader = DirectoryReader.open(getDirectory());
    IndexSearcher searcher = new IndexSearcher(reader);
    Query query = new MultiFieldQueryParser(OLuceneIndexManagerAbstract.LUCENE_VERSION, new String[] { "title" },
        new StandardAnalyzer(OLuceneIndexManagerAbstract.LUCENE_VERSION)).parse("down the");
    final TopDocs docs = searcher.search(query, Integer.MAX_VALUE);
    ScoreDoc[] hits = docs.scoreDocs;
    List<ODocument> oDocs = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>(
        "select *,$score from Song where title LUCENE \"down the\""));
    Assert.assertEquals(oDocs.size(), hits.length);

    int i = 0;
    for (ScoreDoc hit : hits) {
      Assert.assertEquals(oDocs.get(i).field("$score"), hit.score);
      i++;
    }
    reader.close();

  }

  protected String getScriptFromStream(InputStream in) {
    String script = "";
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(in));
      StringBuilder out = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        out.append(line + "\n");
      }
      script = out.toString();
      reader.close();
    } catch (Exception e) {

    }
    return script;
  }

  @AfterClass
  public void deInit() {
    deInitDB();
    OLuceneIndexUtils.deleteFolder(getPath());
  }

}
