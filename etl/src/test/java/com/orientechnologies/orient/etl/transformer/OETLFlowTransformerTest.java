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

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.etl.OETLBaseTest;
import org.junit.Test;

/**
 * Tests ETL Flow Transformer.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OETLFlowTransformerTest extends OETLBaseTest {
  @Test
  public void testSkip() {

    configure(
        "{source: { content: { value: 'name,surname\nJay,Miner\nSkipMe,Test' } }, extractor : { csv: {} },"
            + " transformers: [{vertex: {class:'V'}}, "
            + "{flow:{operation:'skip',if: 'name <> \'Jay\''}},"
            + "{field:{fieldName:'name', value:'3'}}"
            + "], loader: { orientdb: { dbURL: 'memory:"
            + name.getMethodName()
            + "', dbType:'graph' } } }");
    proc.execute();
    ODatabaseDocument db = proc.getLoader().getPool().acquire();

    assertEquals(1, db.countClass("V"));

    OResultSet resultSet = db.query("SELECT FROM V");
    OResult v1 = resultSet.next();
    Object value1 = v1.getProperty("name");
    assertEquals("3", value1);
    resultSet.close();
  }

  @Test
  public void testSkipNever() {
    configure(
        "{source: { content: { value: 'name,surname\nJay,Miner\nTest,Test' } }, "
            + "extractor : { csv: {} },"
            + " transformers: ["
            + "{vertex: {class:'V'}}, "
            + "{flow:{operation:'skip',if: 'name = \'Jay\''}},"
            + "{field:{fieldName:'name', value:'3'}}"
            + "],"
            + " loader: { orientdb: {  dbURL: 'memory:"
            + name.getMethodName()
            + "', dbType:'graph'} } }");

    proc.execute();
    ODatabaseDocument db = proc.getLoader().getPool().acquire();

    assertEquals(1, db.countClass("V"));

    OResultSet resultSet = db.query("SELECT FROM V");
    OResult v1 = resultSet.next();
    Object value1 = v1.getProperty("name");
    assertEquals("3", value1);
    Object value2 = v1.getProperty("surname");
    assertEquals("Test", value2);
    resultSet.close();
  }
}
