package com.orientechnologies.orient.etl;

import org.assertj.core.api.Assertions;
import org.junit.Test;

/**
 * Created by frank on 9/18/15.
 */
public class OETLProcessorTest {

    @Test
    public void testMain() throws Exception {

        final OETLProcessor processor = OETLProcessor.parseConfigAndParameters(new String[]{"-dburl=local:/tmp/db","./src/main/resources/comment.json"});

        Assertions.assertThat(processor.getContext().getVariable("dburl")).isEqualTo("local:/tmp/db");

    }
}