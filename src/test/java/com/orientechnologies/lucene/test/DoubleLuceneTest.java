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
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Created by Enrico Risa on 18/09/15.
 */
@Test
public class DoubleLuceneTest {

  @Test
  public void testDoubleLucene() {
    OrientGraphNoTx graph = new OrientGraphNoTx("memory:doubleLucene");
    ODatabaseDocumentTx db = graph.getRawGraph();

    db.command(new OCommandSQL("create class Test extends V")).execute();
    db.command(new OCommandSQL("create property Test.attr1 string")).execute();
    db.command(new OCommandSQL("create index Test.attr1 on Test (attr1) fulltext engine lucene")).execute();
    db.command(new OCommandSQL("create property Test.attr2 string")).execute();
    db.command(new OCommandSQL("create index Test.attr2 on Test (attr2) fulltext engine lucene")).execute();
    db.command(new OCommandSQL("insert into Test set attr1='foo', attr2='bar'")).execute();
    db.command(new OCommandSQL("insert into Test set attr1='bar', attr2='foo'")).execute();

    List<ODocument> results = db.command(new OCommandSQL("select from Test where attr1 lucene 'foo*' OR attr2 lucene 'foo*'"))
        .execute();
    Assert.assertEquals(results.size(), 2);

    results = db.command(new OCommandSQL("select from Test where attr1 lucene 'bar*' OR attr2 lucene 'bar*'")).execute();

    Assert.assertEquals(results.size(), 2);

    results = db.command(new OCommandSQL("select from Test where attr1 lucene 'foo*' AND attr2 lucene 'bar*'")).execute();

    Assert.assertEquals(results.size(), 1);

    results = db.command(new OCommandSQL("select from Test where attr1 lucene 'bar*' AND attr2 lucene 'foo*'")).execute();

    Assert.assertEquals(results.size(), 1);

  }
}
