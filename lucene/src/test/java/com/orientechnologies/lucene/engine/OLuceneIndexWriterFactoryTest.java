package com.orientechnologies.lucene.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.File;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LiveIndexWriterConfig;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Test;

/** Created by frank on 05/01/2016. */
public class OLuceneIndexWriterFactoryTest {

  @Test
  public void shouldCreateIndexWriterConfiguredWithMetadataValues() throws Exception {

    OLuceneIndexWriterFactory fc = new OLuceneIndexWriterFactory();

    // sample metadata json
    ODocument meta =
        new ODocument()
            .fromJSON(
                OIOUtils.readFileAsString(
                    new File("./src/test/resources/index_metadata_new.json")));

    IndexWriter writer = fc.createIndexWriter(new RAMDirectory(), meta, new StandardAnalyzer());

    LiveIndexWriterConfig config = writer.getConfig();
    assertThat(config.getUseCompoundFile()).isFalse();

    assertThat(config.getAnalyzer()).isInstanceOf(StandardAnalyzer.class);

    assertThat(config.getMaxBufferedDocs()).isEqualTo(-1);

    assertThat(config.getRAMPerThreadHardLimitMB()).isEqualTo(1024);
  }
}
