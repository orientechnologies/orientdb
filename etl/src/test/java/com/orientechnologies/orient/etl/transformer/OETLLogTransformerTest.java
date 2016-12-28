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

package com.orientechnologies.orient.etl.transformer;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.etl.OETLBaseTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.logging.ConsoleHandler;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;

public class OETLLogTransformerTest extends OETLBaseTest {

  private PrintStream           sysOut;
  private ByteArrayOutputStream output;

  @Before
  public void redirectSysOutToByteBuff() {

    sysOut = System.err;
    output = new ByteArrayOutputStream();
    System.setErr(new PrintStream(output, true));

    //install a new console handler that writes on byteArray
    Logger.getLogger("").addHandler(new ConsoleHandler());

    //install custom formatter for new handler
    OLogManager.instance().installCustomFormatter();
  }

  @After
  public void redirecByteBuffToSysout() {
    System.setErr(sysOut);
  }

  @Test
  public void testPrefix() throws Exception {
    String cfgJson = "{source: { content: { value: 'id,text\n1,Hello\n2,Bye'} }, " + "extractor : { csv: {} }, "
        + "transformers : [{ log : {prefix:'-> '}}], " + "loader : { test: {} } }";
    process(cfgJson);
    String[] stringList = output.toString().split(System.getProperty("line.separator"));

    assertThat(stringList[3]).contains("-> {id:1,text:Hello}");
    assertThat(stringList[4]).contains("-> {id:2,text:Bye}");

  }

  @Test
  public void testPostfix() throws Exception {
    String cfgJson = "{source: { content: { value: 'id,text\n1,Hello\n2,Bye'} }, " + "extractor : { csv : {} }, "
        + "transformers : [{ log : {postfix:'-> '}}], " + "loader : { test: {} } }";
    process(cfgJson);

    String[] stringList = output.toString().split(System.getProperty("line.separator"));

    assertThat(stringList[3]).contains("{id:1,text:Hello}->");
    assertThat(stringList[4]).contains("{id:2,text:Bye}->");

  }

}