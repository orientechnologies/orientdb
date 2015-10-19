package com.orientechnologies.orient.etl;

import com.orientechnologies.orient.etl.transformer.OVertexTransformer;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by frank on 9/18/15.
 */
public class OETLProcessorTest {

  @Test public void testMain() throws Exception {

    final OETLProcessor processor = OETLProcessor
        .parseConfigAndParameters(new String[] { "-dburl=local:/tmp/db", "./src/test/resources/comment.json" });

    assertThat(processor.getContext().getVariable("dburl")).isEqualTo("local:/tmp/db");

  }

  @Test public void shouldParseSplittedConfiguration() throws Exception {

    final OETLProcessor processor = OETLProcessor.parseConfigAndParameters(
        new String[] { "-dburl=local:/tmp/db", "./src/test/resources/comment_split_1.json",
            "./src/test/resources/comment_split_2.json" });

    assertThat(processor.getContext().getVariable("dburl")).isEqualTo("local:/tmp/db");
    assertThat(processor.getTransformers().get(0)).isInstanceOf(OVertexTransformer.class);
    assertThat(processor.getExtractor().getName()).isEqualTo("csv");
  }

  @Test public void shouldExceuteBeginBlocktoExpandVariables() throws Exception {

    final OETLProcessor processor = OETLProcessor.parseConfigAndParameters(new String[] { "./src/test/resources/comment.json" });

    assertThat(processor.context.getVariable("filePath")).isEqualTo("./src/test/resources/comments.csv");

  }

}