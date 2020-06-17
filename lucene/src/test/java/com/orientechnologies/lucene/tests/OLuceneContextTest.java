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

import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;

/** Created by enricorisa on 08/10/14. */
public class OLuceneContextTest extends OLuceneBaseTest {

  @Before
  public void init() {
    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");

    db.execute("sql", getScriptFromStream(stream));

    db.command("create index Song.title on Song (title) FULLTEXT ENGINE LUCENE");
  }

  @Test
  public void shouldReturnScore() {

    OResultSet docs =
        db.query("select *,$score from Song where search_index('Song.title', 'title:man')= true ");

    List<OResult> results = docs.stream().collect(Collectors.toList());

    assertThat(results).hasSize(14);
    Float latestScore = 100f;

    // results are ordered by score desc
    for (OResult doc : results) {
      Float score = doc.getProperty("$score");
      assertThat(score).isNotNull().isLessThanOrEqualTo(latestScore);
      latestScore = score;
    }
    docs.close();
  }

  @Test
  public void shouldReturnTotalHits() throws Exception {
    OResultSet docs =
        db.query(
            "select *,$totalHits,$Song_title_totalHits from Song where search_class('title:man')= true  limit 1");

    List<OResult> results = docs.stream().collect(Collectors.toList());
    assertThat(results).hasSize(1);

    OResult doc = results.get(0);
    System.out.println("doc.toElement().toJSON() = " + doc.toElement().toJSON());

    assertThat(doc.<Long>getProperty("$totalHits")).isEqualTo(14l);
    assertThat(doc.<Long>getProperty("$Song_title_totalHits")).isEqualTo(14l);
    docs.close();
  }
}
