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

package com.orientechnologies.lucene;

import com.orientechnologies.lucene.test.BaseLuceneTest;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by enricorisa on 26/09/14.
 */

public class LuceneCrossClassIndexTest extends BaseLuceneTest {

  @Before
  public void setUp() throws Exception {
    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");

    db.execute("sql", getScriptFromStream(stream));

    db.command(
        "create index Song.title on Song (title) FULLTEXT ENGINE LUCENE METADATA {\"analyzer\":\"" + StandardAnalyzer.class
            .getName() + "\"}");
    db.command(
        "create index Song.author on Song (author) FULLTEXT ENGINE LUCENE METADATA {\"analyzer\":\"" + StandardAnalyzer.class
            .getName() + "\"}");
    db.command(
        "create index Author.name on Author(name,score) FULLTEXT ENGINE LUCENE METADATA {\"analyzer\":\"" + StandardAnalyzer.class
            .getName() + "\"}");

  }

  @Test
  public void shouldSearchTermAcrossAllSubIndexes() throws Exception {

    String query = "select expand(search('mountain'))";

    OResultSet resultSet = db.query(query);

    fetchElements(resultSet)
        .forEach(el -> {

          if (el.getSchemaType().get().getName().equals("Song"))
            assertThat(el.<String>getProperty("title")).containsIgnoringCase("mountain");

          if (el.getSchemaType().get().getName().equals("Author"))
            assertThat(el.<String>getProperty("name")).containsIgnoringCase("mountain");

        });

  }

  private Stream<OElement> fetchElements(OResultSet resultSet) {
    return resultSet.stream()
        .map(OResult::getElement)
        .filter(Optional::isPresent)
        .map(Optional::get);

  }

  @Test
  public void shouldSearchAcrossAllSubIndexesWithStrictQuery() {

    String query = "select expand(SEARCH('Song.title:mountain Author.name:Chuck') )";
    OResultSet resultSet = db.query(query);

    fetchElements(resultSet)
        .forEach(el -> {

          if (el.getSchemaType().get().getName().equals("Song"))
            assertThat(el.<String>getProperty("title")).containsIgnoringCase("mountain");

          if (el.getSchemaType().get().getName().equals("Author"))
            assertThat(el.<String>getProperty("name")).containsIgnoringCase("chuck");

        });
  }

  @Test
  public void shouldSearchAcrossAllClassesWithRangeQuery() {

    String query = "select expand(SEARCH('Song.title:mountain Author.score:[4 TO 7]')) ";
    OResultSet resultSet = db.query(query);

    fetchElements(resultSet)
        .forEach(el -> {
          if (el.getSchemaType().get().getName().equals("Song"))
            assertThat(el.<String>getProperty("title")).containsIgnoringCase("mountain");

          if (el.getSchemaType().get().getName().equals("Author"))
            assertThat(el.<Integer>getProperty("score")).isGreaterThan(4).isLessThan(7);
        });
  }

}
