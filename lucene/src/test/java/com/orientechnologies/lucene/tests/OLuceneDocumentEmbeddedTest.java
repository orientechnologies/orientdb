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

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

/** Created by enricorisa on 03/09/14. */
public class OLuceneDocumentEmbeddedTest extends OLuceneBaseTest {

  @Before
  public void init() {
    OClass type = db.getMetadata().getSchema().createClass("City");
    type.createProperty("name", OType.STRING);

    db.command("create index City.name on City (name) FULLTEXT ENGINE LUCENE");
  }

  @Test
  public void embeddedNoTx() {

    ODocument doc = new ODocument("City");

    doc.field("name", "London");
    db.save(doc);

    doc = new ODocument("City");
    doc.field("name", "Rome");

    db.save(doc);

    OResultSet results =
        db.command("select from City where SEARCH_FIELDS(['name'] ,'London') = true ");

    Assertions.assertThat(results).hasSize(1);
  }

  @Test
  public void embeddedTx() {

    ODocument doc = new ODocument("City");

    db.begin();
    doc.field("name", "Berlin");

    db.save(doc);

    db.commit();

    OResultSet results =
        db.command("select from City where SEARCH_FIELDS(['name'] ,'Berlin')=true ");

    Assertions.assertThat(results).hasSize(1);
  }
}
