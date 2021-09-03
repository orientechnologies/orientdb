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

import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.io.InputStream;
import org.junit.Before;
import org.junit.Test;

/** Created by enricorisa on 19/09/14. */
public class OLuceneSingleFieldEmbeddedTest extends OLuceneBaseTest {

  @Before
  public void init() {
    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");

    db.execute("sql", getScriptFromStream(stream));

    db.command("create index Song.title on Song (title) FULLTEXT ENGINE LUCENE");
    db.command("create index Song.author on Song (author) FULLTEXT ENGINE LUCENE");
  }

  @Test
  public void loadAndTest() {

    OResultSet docs =
        db.query("select * from Song where search_fields(['title'],\"(title:mountain)\")=true");

    assertThat(docs).hasSize(4);
    docs.close();

    docs = db.query("select * from Song where search_fields(['author'],\"(author:Fabbio)\")=true");

    assertThat(docs).hasSize(87);
    docs.close();
    docs =
        db.query(
            "select * from Song where search_fields(['title'],\"(title:mountain)\")=true  and search_fields(['author'],\"(author:Fabbio)\")=true");

    assertThat(docs).hasSize(1);
    docs.close();
  }
}
