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

import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by enricorisa on 08/10/14.
 */
public class OLuceneContextTest extends OLuceneBaseTest {

  @Before
  public void init() {
    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");

    db.command(new OCommandScript("sql", getScriptFromStream(stream))).execute();

    db.command(new OCommandSQL("create index Song.title on Song (title) FULLTEXT ENGINE LUCENE")).execute();

    db.command(new OCommandSQL("create index Song.author on Song (author) FULLTEXT ENGINE LUCENE")).execute();

  }

  @Test
  @Ignore
  public void testContext() {

    OResultSet docs = db
        .query("select *,$score from Song where search_index('Song.title', 'title:man')= true ");

    List<OResult> results = docs.stream().collect(Collectors.toList());

    assertThat(results).hasSize(14);
    Float latestScore = 100f;
    for (OResult doc : results) {
      Float score = doc.getProperty("$score");
      assertThat(score)
          .isNotNull()
          .isLessThan(latestScore);
      latestScore = score;
    }

    docs = db.query(
        "select *,$totalHits,$Song_title_totalHits search_index('Song.title', 'title:man')= true  limit 1");

    results = docs.stream().collect(Collectors.toList());
    assertThat(results).hasSize(1);

    OResult doc = results.get(0);
    Assert.assertEquals(doc.<Object>getProperty("$totalHits"), 14);
    Assert.assertEquals(doc.<Object>getProperty("$Song_title_totalHits"), 14);

  }

}
