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

import static org.junit.Assert.assertFalse;

import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.io.InputStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Created by enricorisa on 08/10/14. */
public class LuceneContextTest extends BaseLuceneTest {

  @Before
  public void init() {
    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");

    db.execute("sql", getScriptFromStream(stream)).close();

    db.command("create index Song.title on Song (title) FULLTEXT ENGINE LUCENE").close();
    db.command("create index Song.author on Song (author) FULLTEXT ENGINE LUCENE").close();
  }

  @Test
  public void testContext() {

    OResultSet docs =
        db.query(
            "select *,$score from Song where [title] LUCENE \"(title:man)\" order by $score desc");

    Float latestScore = 100f;
    int count = 0;
    while (docs.hasNext()) {
      count++;
      OResult doc = docs.next();
      Float score = doc.getProperty("$score");
      Assert.assertNotNull(score);
      Assert.assertTrue(score <= latestScore);
      latestScore = score;
    }
    Assert.assertEquals(count, 14);

    docs =
        db.query(
            "select *,$totalHits,$Song_title_totalHits from Song where [title] LUCENE \"(title:man)\" limit 1");

    OResult doc = docs.next();
    Assert.assertEquals(new Long(14), doc.<Long>getProperty("$totalHits"));
    Assert.assertEquals(new Long(14), doc.<Long>getProperty("$Song_title_totalHits"));
    assertFalse(docs.hasNext());
  }
}
