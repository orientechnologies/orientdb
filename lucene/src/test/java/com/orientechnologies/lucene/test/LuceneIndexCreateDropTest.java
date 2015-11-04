/*
 *
 *  * Copyright 2014 Orient Technologies.
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

package com.orientechnologies.lucene.test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Created by Enrico Risa on 14/08/15.
 */
@Test
public class LuceneIndexCreateDropTest extends BaseLuceneTest {

  public LuceneIndexCreateDropTest() {
  }

  public LuceneIndexCreateDropTest(boolean remote) {
    super();
  }

  @Override
  protected String getDatabaseName() {
    return "createDrop";
  }

  @BeforeClass
  public void init() {
    initDB();

    OrientGraph graph = new OrientGraph((ODatabaseDocumentTx) databaseDocumentTx, false);
    OrientVertexType type = graph.createVertexType("City");
    type.createProperty("name", OType.STRING);

    databaseDocumentTx.command(new OCommandSQL("create index City.name on City (name) FULLTEXT ENGINE LUCENE")).execute();
  }

  @Test
  public void dropIndex() {
    databaseDocumentTx.command(new OCommandSQL("drop index City.name")).execute();
  }

  @AfterClass
  public void deInit() {
    deInitDB();
  }
}
