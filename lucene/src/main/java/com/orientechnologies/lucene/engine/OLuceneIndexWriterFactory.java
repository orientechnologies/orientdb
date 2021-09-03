package com.orientechnologies.lucene.engine;

import static org.apache.lucene.index.IndexWriterConfig.OpenMode.CREATE_OR_APPEND;

import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.IOException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;

/** Created by frank on 05/01/2016. */
public class OLuceneIndexWriterFactory {

  public IndexWriter createIndexWriter(Directory dir, ODocument metadata, Analyzer analyzer)
      throws IOException {

    IndexWriterConfig config = createIndexWriterConfig(metadata, analyzer);

    return new IndexWriter(dir, config);
  }

  public IndexWriterConfig createIndexWriterConfig(ODocument metadata, Analyzer analyzer) {
    IndexWriterConfig config = new IndexWriterConfig(analyzer);

    config.setOpenMode(CREATE_OR_APPEND);

    if (metadata.containsField("use_compound_file"))
      config.setUseCompoundFile(metadata.<Boolean>field("use_compound_file"));

    if (metadata.containsField("ram_buffer_MB"))
      config.setRAMBufferSizeMB(Double.valueOf(metadata.<String>field("ram_buffer_MB")));

    if (metadata.containsField("max_buffered_docs"))
      config.setMaxBufferedDocs(Integer.valueOf(metadata.<String>field("max_buffered_docs")));

    // TODO REMOVED

    //    if (metadata.containsField("max_buffered_delete_terms"))
    //
    // config.setMaxBufferedDeleteTerms(Integer.valueOf(metadata.<String>field("max_buffered_delete_terms")));

    if (metadata.containsField("ram_per_thread_MB"))
      config.setRAMPerThreadHardLimitMB(
          Integer.valueOf(metadata.<String>field("ram_per_thread_MB")));

    return config;
  }
}
