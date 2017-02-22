package com.orientechnologies.lucene;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by frank on 14/11/2016.
 */
public class OLuceneIndexSearcher {

  private final List<IndexReader> readers;
  private       IndexReader       reader;

  public OLuceneIndexSearcher(String indexBasePath) {
    Path path = Paths.get(indexBasePath);

    readers = new ArrayList<>();
    try {
      try (DirectoryStream<Path> paths = Files.newDirectoryStream(path)) {

        for (Path indexPAth : paths) {
          readers.add(getIndexReader(createDirectory(indexPAth)));
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private IndexReader getIndexReader(Directory d) {
    try {
      return DirectoryReader.open(d);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Directory createDirectory(Path path) {

    try {
      return FSDirectory.open(path);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public IndexReader reader() {
    try {
      if (readers.size() > 1) {

        reader = new MultiReader(readers.toArray(new IndexReader[] {}));
      } else {
        reader = readers.get(0);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    System.out.println("documents:: " + reader.numDocs() + " from " + readers.size() + " subIndexes");
    return reader;
  }

  public TopDocs search(String luceneQuery, int topDocs) {

    try {
      IndexSearcher searcher = new IndexSearcher(reader);

      Query query = new QueryParser("reviewText", new StandardAnalyzer()).parse(luceneQuery);

      return searcher.search(query, topDocs);
    } catch (ParseException | IOException e) {
      throw new RuntimeException(e);
    }

  }
}
