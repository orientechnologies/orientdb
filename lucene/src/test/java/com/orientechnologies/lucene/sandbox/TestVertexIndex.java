package com.orientechnologies.lucene.sandbox;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.RAMDirectory;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * Created by frank on 9/28/15.
 */
public class TestVertexIndex {

  @Test
  public void testFullTextIndexOnvertex() {

    OrientGraph graph = new OrientGraph("memory:TestDB", "admin", "admin");
    OrientVertexType vType = graph.getVertexType("V");

    vType.createProperty("title", OType.STRING);
    vType.createProperty("text", OType.STRING);

    vType.createIndex("V.", "FULLTEXT", null, null, "LUCENE", new String[] { "title", "text" });

    graph.shutdown();
  }

  @Test
  public void testSpacesInQuery() throws IOException, ParseException {

    IndexWriterConfig conf = new IndexWriterConfig(new StandardAnalyzer());
    final RAMDirectory directory = new RAMDirectory();
    final IndexWriter writer = new IndexWriter(directory, conf);

    Document doc = new Document();
    doc.add(new TextField("name", "Max Water", Field.Store.YES));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new TextField("name", "Max Waterson", Field.Store.YES));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new TextField("name", "Cory Watney", Field.Store.YES));
    writer.addDocument(doc);

    writer.commit();


    IndexReader reader = DirectoryReader.open(directory);

    IndexSearcher searcher = new IndexSearcher(reader);

    Analyzer analyzer = new StandardAnalyzer();

    QueryParser queryParser = new QueryParser("name", analyzer);

    final Query query = queryParser.parse("name:Max AND name:Wat*");


    final TopDocs topDocs = searcher.search(query, 10);

    System.out.println("total docs:: " + topDocs.totalHits);
    for (int i = 0; i < topDocs.totalHits; i++){

      final Document found = searcher.doc(topDocs.scoreDocs[i].doc);

      System.out.println("found:: " + found.get("name"));

    }

    reader.close();
    writer.close();
  }

}
