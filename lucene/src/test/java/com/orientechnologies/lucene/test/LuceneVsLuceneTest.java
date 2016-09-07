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

import com.orientechnologies.common.io.OFileUtils;
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
import org.apache.lucene.document.TextField;
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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Created by enricorisa on 08/10/14.
 */
public class LuceneVsLuceneTest extends BaseLuceneTest {

  private IndexWriter indexWriter;

  @Before
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
      Analyzer analyzer = new StandardAnalyzer();
      IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
      iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
      indexWriter = new IndexWriter(dir, iwc);

    } catch (IOException e) {
      e.printStackTrace();
    }
    databaseDocumentTx.command(new OCommandSQL("create index Song.title on Song (title) FULLTEXT ENGINE LUCENE")).execute();

  }

  protected Directory getDirectory() throws IOException {
    return NIOFSDirectory.open(getPath().toPath());
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

        d.add(new TextField("title", title, Field.Store.NO));
        indexWriter.addDocument(d);

      }
    }

    indexWriter.close();
    IndexReader reader = DirectoryReader.open(getDirectory());
    IndexSearcher searcher = new IndexSearcher(reader);
    Query query = new MultiFieldQueryParser(new String[] { "title" }, new StandardAnalyzer()).parse("down the");
    final TopDocs docs = searcher.search(query, Integer.MAX_VALUE);
    ScoreDoc[] hits = docs.scoreDocs;
    List<ODocument> oDocs = databaseDocumentTx
        .query(new OSQLSynchQuery<ODocument>("select *,$score from Song where title LUCENE \"down the\""));
    Assert.assertEquals(oDocs.size(), hits.length);

    int i = 0;
    for (ScoreDoc hit : hits) {
      Assert.assertEquals(oDocs.get(i).<Object>field("$score"), hit.score);
      i++;
    }
    reader.close();

  }

  @After
  public void deInit() {
    deInitDB();
    OFileUtils.deleteRecursively(getPath());
  }

}
