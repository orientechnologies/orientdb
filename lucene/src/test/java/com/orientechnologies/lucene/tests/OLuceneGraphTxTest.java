/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

package com.orientechnologies.lucene.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Before;
import org.junit.Test;

/** Created by enricorisa on 28/06/14. */
public class OLuceneGraphTxTest extends OLuceneBaseTest {

  @Before
  public void init() {
    OClass type = db.createVertexClass("City");
    type.createProperty("name", OType.STRING);

    db.command("create index City.name on City (name) FULLTEXT ENGINE LUCENE");
  }

  @Test
  public void graphTxTest() throws Exception {

    OVertex v = db.newVertex("City");
    v.setProperty("name", "London");

    // save london
    db.save(v);

    OResultSet resultSet = db.command("select from City where search_class('London') =true ");

    assertThat(resultSet).hasSize(1);

    // modifiy vertex
    v.setProperty("name", "Berlin");

    // re-save
    db.save(v);

    // only berlin
    resultSet = db.command("select from City where search_class('Berlin') =true ");
    assertThat(resultSet).hasSize(1);

    resultSet = db.command("select from City where search_class('London') =true ");
    assertThat(resultSet).hasSize(0);

    db.commit();

    //
    resultSet = db.command("select from City where search_class('Berlin') =true ");
    assertThat(resultSet).hasSize(1);

    resultSet = db.command("select from City where search_class('London') =true ");
    assertThat(resultSet).hasSize(0);
  }
}
