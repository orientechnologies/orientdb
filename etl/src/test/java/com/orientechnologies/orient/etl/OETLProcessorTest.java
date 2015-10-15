package com.orientechnologies.orient.etl;

import com.orientechnologies.orient.etl.transformer.OCSVTransformer;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * Created by frank on 9/18/15.
 */
public class OETLProcessorTest {

  @Test
  public void testMain() throws Exception {

    final OETLProcessor processor = OETLProcessor.parseConfigAndParameters(new String[] { "-dburl=local:/tmp/db",
        "./src/main/resources/comment.json" });

    assertThat(processor.getContext().getVariable("dburl")).isEqualTo("local:/tmp/db");

  }

  @Test
  public void shouldParseSplittedConfiguration() throws Exception {

    final OETLProcessor processor = OETLProcessor.parseConfigAndParameters(new String[] { "-dburl=local:/tmp/db",
        "./src/main/resources/comment_split_1.json", "./src/main/resources/comment_split_2.json" });

    assertThat(processor.getContext().getVariable("dburl")).isEqualTo("local:/tmp/db");
    assertThat(processor.getTransformers().get(0)).isInstanceOf(OCSVTransformer.class);
    assertThat(processor.getExtractor().getName()).isEqualTo("row");
  }

}