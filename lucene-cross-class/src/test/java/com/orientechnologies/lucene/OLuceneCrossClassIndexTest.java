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

import com.orientechnologies.lucene.tests.OLuceneBaseTest;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by enricorisa on 26/09/14.
 */

public class OLuceneCrossClassIndexTest extends OLuceneBaseTest {

  @Before
  public void setUp() throws Exception {
    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");

    db.execute("sql", getScriptFromStream(stream));

    db.command(
        "create index Song.title on Song (title,author) FULLTEXT ENGINE LUCENE METADATA {\"analyzer\":\"" + StandardAnalyzer.class
            .getName() + "\"}");
    db.command(
        "create index Author.name on Author(name,score) FULLTEXT ENGINE LUCENE METADATA {\"analyzer\":\"" + StandardAnalyzer.class
            .getName() + "\"}");

  }

  @Test
  public void shouldSearchTermAcrossAllSubIndexes() throws Exception {

    String query = "select expand(search_CROSS('mountain'))";

    OResultSet resultSet = db.query(query);
    List<OElement> elements = fetchElements(resultSet).collect(Collectors.toList());

    assertThat(elements).isNotEmpty();

    elements.forEach(el -> {

      String className = el.getSchemaType().get().getName();
      if (className.equals("Song"))
        assertThat(el.<String>getProperty("title")).containsIgnoringCase("mountain");

      if (className.equals("Author"))
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

    String query = "select expand(SEARCH_CROSS('Song.title:mountain Author.name:Chuck') )";
    OResultSet resultSet = db.query(query);

    List<OElement> elements = fetchElements(resultSet).collect(Collectors.toList());

    assertThat(elements).isNotEmpty();

    elements.forEach(el -> {

      String className = el.getSchemaType().get().getName();
      if (className.equals("Song"))
        assertThat(el.<String>getProperty("title")).containsIgnoringCase("mountain");

      if (className.equals("Author"))
        assertThat(el.<String>getProperty("name")).containsIgnoringCase("chuck");

    });
  }

  @Test
  public void shouldSearchAcrossAllSubIndexesUsingMetadataFields() {

    String query = "SELECT expand(SEARCH_CROSS('(+_CLASS:Song +title:mountain) (+_CLASS:Author +name:Chuck)') )";
    OResultSet resultSet = db.query(query);

    List<OElement> elements = fetchElements(resultSet).collect(Collectors.toList());

    assertThat(elements).isNotEmpty();

    elements.forEach(el -> {

      String className = el.getSchemaType().get().getName();
      if (className.equals("Song"))
        assertThat(el.<String>getProperty("title")).containsIgnoringCase("mountain");

      if (className.equals("Author"))
        assertThat(el.<String>getProperty("name")).containsIgnoringCase("chuck");

    });
  }

  @Test
  public void shouldSearchAcrossAllClassesWithRangeQuery() {

    String query = "select expand(SEARCH_CROSS('Song.title:mountain  Author.score:[4 TO 7]', {'allowLeadingWildcard' : true})) ";
    OResultSet resultSet = db.query(query);
    List<OElement> elements = fetchElements(resultSet).collect(Collectors.toList());

    assertThat(elements).isNotEmpty();

    elements.forEach(el -> {
      String className = el.getSchemaType().get().getName();
      assertThat(className).isIn("Song", "Author");
      if (className.equals("Song"))
        assertThat(el.<String>getProperty("title")).containsIgnoringCase("mountain");

      if (className.equals("Author"))
        assertThat(el.<Integer>getProperty("score")).isGreaterThanOrEqualTo(4).isLessThanOrEqualTo(7);

    });
  }

  @Test
  public void shouldSearchAcrossExcludingSongIndex() {

    String query = "select expand(SEARCH_CROSS('Song.title:mountain  Author.score:[4 TO 7]', "
        + "{'excludes' : ['Song.title']})) ";
    OResultSet resultSet = db.query(query);
    List<OElement> elements = fetchElements(resultSet).collect(Collectors.toList());

    assertThat(elements).isNotEmpty();

    final List<OElement> song = elements.stream()
        .filter(el -> el.getSchemaType().get().getName().equals("Song"))
        .collect(Collectors.toList());

    assertThat(song).hasSize(0);

  }

  @Test
  public void shouldSearchAcrossAllClassesWithMetedata() {

    String query = "select  expand(SEARCH_CROSS('Author.name:bob Song.title:*tain', {"
        + "'allowLeadingWildcard' : true,"
        + "'boost': {'Author.name':2.0}"
        + "})) ";
    OResultSet resultSet = db.query(query);
    List<OElement> elements = fetchElements(resultSet).collect(Collectors.toList());

    assertThat(elements).isNotEmpty();

    elements.forEach(el -> {
      String className = el.getSchemaType().get().getName();
      assertThat(className).isIn("Song", "Author");
      if (className.equals("Song"))
        assertThat(el.<String>getProperty("title")).containsIgnoringCase("mountain");

      if (className.equals("Author"))
        assertThat(el.<String>getProperty("name")).containsIgnoringCase("bob");

    });
  }

  @Test
  public void shouldSearchAcrossAllClassesWithCustomSort() {

    String query = "select  expand(SEARCH_CROSS('*:* ', {"
        + "sort: [{field:'Author.score', reverse:true, type:'INT' },"
        + "{field:'Song.author', type:'STRING'}]"
        + "})) limit 11";
    OResultSet resultSet = db.query(query);

    List<OElement> elements = fetchElements(resultSet).collect(Collectors.toList());

    assertThat(elements).isNotEmpty();

    //Authors sorted in reverse mode on score
    List<Integer> scores = elements.stream()
        .filter(e -> e.getSchemaType().get().getName().equals("Author"))
        .map(o -> o.<Integer>getProperty("score")).collect(Collectors.toList());

    assertThat(scores).containsExactly(10, 10, 7, 5, 4);

    //song author sorted on name
    List<String> songAuthoer = elements.stream()
        .filter(e -> e.getSchemaType().get().getName().equals("Song"))
        .map(o -> o.<String>getProperty("author")).collect(Collectors.toList());

    assertThat(songAuthoer).startsWith("A.P.Carter",
        "Al Green",
        "Arthur Cruddup",
        "Arthur Cruddup",
        "Arthur Cruddup",
        "B and S Womack");

  }

}
