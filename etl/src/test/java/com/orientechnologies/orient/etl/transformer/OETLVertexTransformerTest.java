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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.etl.OETLBaseTest;
import org.junit.Test;

/**
 * Tests ETL Vertex Transformer.
 *
 * @author Gregor Frey
 */
public class OETLVertexTransformerTest extends OETLBaseTest {

  public void createClasses(ODatabaseDocument db) {

    OClass person = db.createVertexClass("Person");
    person.createProperty("name", OType.STRING);
    person.createIndex("Person.name", OClass.INDEX_TYPE.UNIQUE, "name");

    db.commit();
  }

  @Test
  public void testCreateVertex() {
    configure(
        "{source: { content: { value: 'name,\nGregor' } }, extractor : { csv: {} },"
            + " transformers: [{vertex: {class:'Person', skipDuplicates:false}},"
            + "], loader: { orientdb: { dbAutoCreateProperties:true, cluster: 'custom', dbURL: 'memory:"
            + name.getMethodName()
            + "', dbType:'graph', useLightweightEdges:false } } }");

    ODatabasePool pool = proc.getLoader().getPool();
    ODatabaseDocument db = pool.acquire();
    createClasses(db);
    db.close();

    proc.execute();
    db = pool.acquire();

    assertEquals("V", 1, db.countClass("V"));
    assertEquals("person", 1, db.countClass("Person"));

    assertThat(db.countClusterElements("custom")).isEqualTo(1);
  }

  @Test
  public void testCreateTargetVertexIfNotExists() {
    configure(
        "{source: { content: { value: 'name,idf,parent\nParent,1,\nChild,2,1' } }, extractor : { csv: {} },"
            + " transformers: [{merge: { joinFieldName:'idf', lookup:'V.idf'}}, {vertex: {class:'V'}},"
            + "{edge:{ class: 'E', joinFieldName: 'parent', lookup: 'V.idf', unresolvedLinkAction: 'CREATE' }, if: '$input.parent IS NOT NULL'}"
            + "], loader: { orientdb: { dbURL: 'memory:"
            + name.getMethodName()
            + "', dbType:'graph', useLightweightEdges:false } } }");

    ODatabasePool pool = proc.getLoader().getPool();

    ODatabaseDocument db = pool.acquire();
    createClasses(db);
    db.close();

    proc.execute();
    // VERIFY
    db = pool.acquire();

    assertThat(db.countClass("V")).isEqualTo(2);
    db.close();
  }

  //  @Test(expected = ORecordDuplicatedException.class)
  @Test
  public void testErrorOnDuplicateVertex() {
    configure(
        "{ config: { 'log': 'DEBUG' },  source: { content: { value: 'name,\nGregor\nGregor\nHans' } }, extractor : { csv: {} },"
            + " transformers: [ {vertex: {class:'Person', skipDuplicates:false}},"
            + "], loader: { orientdb: { dbURL: 'memory:"
            + name.getMethodName()
            + "', dbType:'graph', useLightweightEdges:false } } }");

    ODatabasePool pool = proc.getLoader().getPool();
    ODatabaseDocument db = pool.acquire();
    createClasses(db);
    db.close();

    proc.execute();
    // VERIFY
    db = pool.acquire();
    assertThat(db.countClass("V")).isEqualTo(1);
    db.close();
  }

  @Test
  public void testSkipDuplicateVertex() {
    configure(
        "{source: { content: { value: 'name,\nGregor\nGregor\nHans' } }, extractor : { csv: {} },"
            + " transformers: [{vertex: {class:'Person', skipDuplicates:true}},],"
            + " loader: { orientdb: {  dbURL: 'memory:"
            + name.getMethodName()
            + "', dbType:'graph', useLightweightEdges:false } } }");

    ODatabasePool pool = proc.getLoader().getPool();
    ODatabaseDocument db = pool.acquire();
    createClasses(db);
    db.close();
    proc.execute();

    // VERIFY
    db = pool.acquire();
    assertThat(db.countClass("Person")).isEqualTo(2);
    db.close();
  }
}
