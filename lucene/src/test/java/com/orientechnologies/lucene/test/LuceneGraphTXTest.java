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

package com.orientechnologies.lucene.test;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import java.util.Collection;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Created by enricorisa on 28/06/14. */
public class LuceneGraphTXTest extends BaseLuceneTest {

  @Before
  public void init() {
    OClass type = db.createVertexClass("City");
    type.createProperty("name", OType.STRING);

    db.command(new OCommandSQL("create index City.name on City (name) FULLTEXT ENGINE LUCENE"))
        .execute();
  }

  @Test
  public void graphTxTest() throws Exception {

    OVertex v = db.newVertex("City");
    v.setProperty("name", "London");

    db.save(v);

    Collection results =
        db.command(new OCommandSQL("select from City where name lucene 'London'")).execute();
    Assert.assertEquals(results.size(), 1);

    v.setProperty("name", "Berlin");

    v.save();

    results = db.command(new OCommandSQL("select from City where name lucene 'Berlin'")).execute();
    Assert.assertEquals(results.size(), 1);

    results = db.command(new OCommandSQL("select from City where name lucene 'London'")).execute();
    Assert.assertEquals(results.size(), 0);

    db.commit();

    // Assert After Commit
    results = db.command(new OCommandSQL("select from City where name lucene 'Berlin'")).execute();
    Assert.assertEquals(results.size(), 1);
    results = db.command(new OCommandSQL("select from City where name lucene 'London'")).execute();
    Assert.assertEquals(results.size(), 0);
  }
}
