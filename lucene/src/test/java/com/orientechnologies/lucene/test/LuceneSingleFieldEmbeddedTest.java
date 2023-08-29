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

import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.io.InputStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Created by enricorisa on 19/09/14. */
public class LuceneSingleFieldEmbeddedTest extends BaseLuceneTest {

  @Test
  public void loadAndTest() {

    OResultSet docs = db.query("select * from Song where [title] LUCENE \"(title:mountain)\"");

    Assert.assertEquals(docs.stream().count(), 4);

    docs = db.query("select * from Song where [author] LUCENE \"(author:Fabbio)\"");

    Assert.assertEquals(docs.stream().count(), 87);

    // not WORK BECAUSE IT USES only the first index
    // String query = "select * from Song where [title] LUCENE \"(title:mountain)\"  and [author]
    // LUCENE \"(author:Fabbio)\""
    String query =
        "select * from Song where [title] LUCENE \"(title:mountain)\"  and author = 'Fabbio'";
    docs = db.query(query);

    Assert.assertEquals(docs.stream().count(), 1);
  }

  @Before
  public void init() {
    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");

    db.execute("sql", getScriptFromStream(stream)).close();

    db.command("create index Song.title on Song (title) FULLTEXT ENGINE LUCENE").close();
    db.command("create index Song.author on Song (author) FULLTEXT ENGINE LUCENE").close();
  }
}
