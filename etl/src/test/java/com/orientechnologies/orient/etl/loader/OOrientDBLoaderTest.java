package com.orientechnologies.orient.etl.loader;

import com.orientechnologies.orient.core.index.OIndexManagerProxy;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.OETLBaseTest;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * Created by frank on 9/14/15.
 */
public class OOrientDBLoaderTest extends OETLBaseTest {

  @Test
  public void testAddMetadataToIndex() {

    process("{source: { content: { value: 'name,surname\nJay,Miner' } }, extractor : { csv: {} }, loader: { orientdb: {\n"
            + "      dbURL: \"memory:OETLBaseTest\",\n"
            + "      dbUser: \"admin\",\n"
            + "      dbPassword: \"admin\",\n"
            + "      dbAutoCreate: true,\n"
            + "      tx: false,\n"
            + "      batchCommit: 1000,\n"
            + "      wal : true,\n"
            + "      dbType: \"graph\",\n"
            + "      classes: [\n"
            + "        {name:\"Person\", extends: \"V\" },\n"
            + "      ],\n"
            + "      indexes: [{class:\"V\" , fields:[\"surname:String\"], \"type\":\"NOTUNIQUE\", \"metadata\": { \"ignoreNullValues\" : \"false\"}} ]  } } }");

    final OIndexManagerProxy indexManager = graph.getRawGraph().getMetadata().getIndexManager();

    assertThat(indexManager.existsIndex("V.surname")).isTrue();

    final ODocument indexMetadata = indexManager.getIndex("V.surname").getMetadata();
    assertThat(indexMetadata.containsField("ignoreNullValues")).isTrue();
    assertThat(indexMetadata.<String>field("ignoreNullValues")).isEqualTo("false");

  }

}