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

package com.orientechnologies.orient.etl.loader;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.etl.OETLBaseTest;
import java.util.List;
import org.junit.Test;

/** Created by frank on 9/14/15. */
public class OETLOrientDBLoaderTest extends OETLBaseTest {

  @Test(expected = OConfigurationException.class)
  public void shouldFailToManageRemoteServer() throws Exception {

    configure(
        "{source: { content: { value: 'name,surname\nJay,Miner' } }, extractor : { csv: {} }, loader: { orientdb: {\n"
            + "      dbURL: \"remote:sadserver/OETLBaseTest\",\n"
            + "      dbUser: \"admin\",\n"
            + "      dbPassword: \"admin\",\n"
            + "      serverUser: \"admin\",\n"
            + "      serverPassword: \"admin\",\n"
            + "      dbAutoCreate: true,\n"
            + "      tx: false,\n"
            + "      batchCommit: 1000,\n"
            + "      wal : true,\n"
            + "      dbType: \"graph\",\n"
            + "      classes: [\n"
            + "        {name:\"Person\", extends: \"V\" },\n"
            + "      ],\n"
            + "      indexes: [{class:\"V\" , fields:[\"surname:String\"], \"type\":\"NOTUNIQUE\", \"metadata\": { \"ignoreNullValues\" : \"false\"}} ]  } } }");

    proc.execute();
  }

  @Test
  public void testAddMetadataToIndex() {

    configure(
        "{source: { content: { value: 'name,surname\nJay,Miner' } }, extractor : { csv: {} }, loader: { orientdb: {\n"
            + "      dbURL: 'memory:"
            + name.getMethodName()
            + "',\n"
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

    proc.execute();
    ODatabaseDocumentInternal db = (ODatabaseDocumentInternal) proc.getLoader().getPool().acquire();

    final OIndexManagerAbstract indexManager = db.getMetadata().getIndexManagerInternal();

    assertThat(indexManager.existsIndex("V.surname")).isTrue();

    final ODocument indexMetadata = indexManager.getIndex(db, "V.surname").getMetadata();
    assertThat(indexMetadata.containsField("ignoreNullValues")).isTrue();
    assertThat(indexMetadata.<String>field("ignoreNullValues")).isEqualTo("false");
  }

  @Test
  public void testCreateLuceneIndex() {

    configure(
        "{source: { content: { value: 'name,surname\nJay,Miner' } }, extractor : { csv: {} }, "
            + "\"transformers\": [\n"
            + "    {\n"
            + "      \"vertex\": {\n"
            + "        \"class\": \"Person\",\n"
            + "        \"skipDuplicates\": true\n"
            + "      }\n"
            + "    }],"
            + "loader: { orientdb: {\n"
            + "      dbURL: 'memory:"
            + name.getMethodName()
            + "',\n"
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
            + "      indexes: [{class:\"Person\" , fields:[\"surname:String\"], \"type\":\"FULLTEXT\",  \"algorithm\":\"LUCENE\",  \"metadata\": { \"ignoreNullValues\" : \"false\"}} ]  } } }");

    proc.execute();

    ODatabaseDocumentInternal db = (ODatabaseDocumentInternal) proc.getLoader().getPool().acquire();

    final OIndexManagerAbstract indexManager = db.getMetadata().getIndexManagerInternal();

    assertThat(indexManager.existsIndex("Person.surname")).isTrue();

    final OIndex index = indexManager.getIndex(db, "Person.surname");
    final ODocument indexMetadata = index.getMetadata();
    assertThat(index.getAlgorithm()).isEqualTo("LUCENE");

    assertThat(indexMetadata.containsField("ignoreNullValues")).isTrue();
    assertThat(indexMetadata.<String>field("ignoreNullValues")).isEqualTo("false");

    final OResultSet resultSet = db.query("select from Person where search_class('mi*')=true");

    assertThat(resultSet).hasSize(1);
    resultSet.close();
  }

  @Test
  public void shouldSaveDocumentsOnGivenCluster() {

    configure(
        "{source: { content: { value: 'name,surname\nJay,Miner' } }, extractor : { csv: {} }, loader: { orientdb: {\n"
            + "      dbURL: \"memory:"
            + name.getMethodName()
            + "\",\n"
            + "      dbUser: \"admin\",\n"
            + "      dbPassword: \"admin\",\n"
            + "      dbAutoCreate: true,\n"
            + "      cluster : \"myCluster\",\n"
            + "      tx: false,\n"
            + "      batchCommit: 1000,\n"
            + "      wal : true,\n"
            + "      dbType: \"graph\",\n"
            + "      classes: [\n"
            + "        {name:\"Person\", extends: \"V\" },\n"
            + "      ],\n"
            + "      indexes: [{class:\"V\" , fields:[\"surname:String\"], \"type\":\"NOTUNIQUE\", \"metadata\": { \"ignoreNullValues\" : \"false\"}} ]  } } }");
    proc.execute();

    ODatabaseDocument db = proc.getLoader().getPool().acquire();

    int idByName = db.getClusterIdByName("myCluster");

    OResultSet resultSet = db.query("SELECT from V");

    resultSet
        .vertexStream()
        .forEach(v -> assertThat((v.getIdentity()).getClusterId()).isEqualTo(idByName));

    resultSet.close();
    db.close();
  }

  @Test
  public void shouldSaveDocuments() {

    configure(
        "{source: { content: { value: 'name,surname,@class\nJay,Miner,Person' } }, extractor : { csv: {} }, loader: { orientdb: {\n"
            + "      dbURL: 'memory:"
            + name.getMethodName()
            + "',\n"
            + "      dbUser: \"admin\",\n"
            + "      dbPassword: \"admin\",\n"
            + "      dbAutoCreate: true,\n      tx: false,\n"
            + "      batchCommit: 1000,\n"
            + "      wal : false,\n"
            + "      dbType: \"document\" , \"classes\": [\n"
            + "        {\n"
            + "          \"name\": \"Person\"\n"
            + "        },\n"
            + "        {\n"
            + "          \"name\": \"UpdateDetails\"\n"
            + "        }\n"
            + "      ]      } } }");

    proc.execute();

    ODatabaseDocument db = proc.getLoader().getPool().acquire();

    List<?> res = db.query(new OSQLSynchQuery<ODocument>("SELECT FROM Person"));

    assertThat(res.size()).isEqualTo(1);

    db.close();
  }

  @Test
  public void shouldSaveDocumentsWithPredefinedSchema() {

    // configure
    configure(
        "{source: { content: { value: 'name,surname,married,birthday\nJay,Miner,false,1970-01-01 05:30:00' } }, "
            + "extractor : { csv: {columns:['name:string','surname:string','married:boolean','birthday:datetime'], dateFormat :'yyyy-MM-dd HH:mm:ss'} }, loader: { orientdb: {\n"
            + "      dbURL: 'memory:"
            + name.getMethodName()
            + "', class:'Person',     dbUser: \"admin\",\n"
            + "      dbPassword: \"admin\",\n"
            + "      dbAutoCreate: false,\n      tx: false,\n"
            + "      batchCommit: 1000,\n"
            + "      wal : false,\n"
            + "      dbType: \"document\" } } }");

    // create class
    ODatabaseDocument db = proc.getLoader().getPool().acquire();
    db.command("CREATE Class Person");
    db.command("CREATE property Person.name STRING");
    db.command("CREATE property Person.surname STRING");
    db.command("CREATE property Person.married BOOLEAN");
    db.command("CREATE property Person.birthday DATETIME");

    db.close();
    // import data
    proc.execute();

    db = proc.getLoader().getPool().acquire();

    OResultSet res = db.query("SELECT FROM Person");

    assertThat(res).hasSize(1);

    res.close();
    db.close();
  }
}
