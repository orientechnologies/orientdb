/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (info(-at-)orientdb.com)
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

package com.orientechnologies.orient.etl;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.etl.loader.OETLLoader;
import com.orientechnologies.orient.etl.loader.OETLOrientDBLoader;
import com.orientechnologies.orient.etl.transformer.OETLVertexTransformer;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by frank on 9/18/15.
 */
public class OETLProcessorTest extends OETLBaseTest {

  @Before
  public void setUp() throws Exception {
    OFileUtils.deleteRecursively(new File("./target/databases/"));

  }

  @Test
  public void testMain() throws Exception {

    final OETLProcessor processor = new OETLProcessorConfigurator()
        .parseConfigAndParameters(new String[] {
            "-dbURL=plocal:/tmp/testMain",
            "./src/test/resources/comment.json" });

    assertThat(processor.getContext().getVariable("dbURL")).isEqualTo("plocal:/tmp/testMain");

    OETLOrientDBLoader loader = (OETLOrientDBLoader) processor.getLoader();
    loader.getPool().close();

    loader.orient.close();
  }

  @Test
  public void shouldParseSplitConfiguration() throws Exception {

    final OETLProcessor processor = new OETLProcessorConfigurator()
        .parseConfigAndParameters(new String[] {
            "-dbURL=plocal:/tmp/shouldParseSplitConfiguration",
            "./src/test/resources/comment_split_1.json",
            "./src/test/resources/comment_split_2.json" });

    assertThat(processor.getContext().getVariable("dbURL")).isEqualTo("plocal:/tmp/shouldParseSplitConfiguration");
    assertThat(processor.getTransformers().get(0)).isInstanceOf(OETLVertexTransformer.class);
    assertThat(processor.getExtractor().getName()).isEqualTo("csv");
    OETLOrientDBLoader loader = (OETLOrientDBLoader) processor.getLoader();
    loader.getPool().close();

    loader.orient.close();
  }

  @Test
  public void shouldExceuteBeginBlocktoExpandVariables() throws Exception {

    final OETLProcessor processor = new OETLProcessorConfigurator()
        .parseConfigAndParameters(new String[] { "-dbURL=plocal:/tmp/shouldExceuteBeginBlocktoExpandVariables",
            "./src/test/resources/comment.json" });

    assertThat(processor.context.getVariable("filePath")).isEqualTo("./src/test/resources/comments.csv");
    OETLOrientDBLoader loader = (OETLOrientDBLoader) processor.getLoader();
    loader.getPool().close();

    loader.orient.close();

  }

}
