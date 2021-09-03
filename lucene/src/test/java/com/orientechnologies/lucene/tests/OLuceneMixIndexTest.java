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

/** Created by Enrico Risa on 02/09/15. */
public class OLuceneMixIndexTest extends OLuceneBaseTest {

  @Before
  public void initLocal() {

    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");

    db.execute("sql", getScriptFromStream(stream));

    db.command("create index Song.author on Song (author) NOTUNIQUE");

    db.command("create index Song.composite on Song (title,lyrics) FULLTEXT ENGINE LUCENE");
  }

  @Test
  public void testMixQuery() {

    OResultSet docs =
        db.query(
            "select * from Song where  author = 'Hornsby' and search_index('Song.composite','title:mountain')=true ");

    assertThat(docs).hasSize(1);
    docs.close();
    docs =
        db.query(
            "select * from Song where  author = 'Hornsby' and search_index('Song.composite','title:ballad')=true");
    assertThat(docs).hasSize(0);
    docs.close();
  }

  @Test
  public void testMixCompositeQuery() {

    OResultSet docs =
        db.query(
            "select * from Song where  author = 'Hornsby' and search_index('Song.composite','title:mountain')=true ");
    assertThat(docs).hasSize(1);
    docs.close();
    docs =
        db.query(
            "select * from Song where author = 'Hornsby' and search_index('Song.composite','lyrics:happy')=true ");

    assertThat(docs).hasSize(1);
    docs.close();
  }
}
