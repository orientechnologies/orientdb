package com.orientechnologies.lucene.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;

public class OLuceneSortTest extends OLuceneBaseTest {

  @Before
  public void setUp() throws Exception {
    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");

    db.execute("sql", getScriptFromStream(stream));

    db.command("create index Song.title on Song (title) FULLTEXT ENGINE LUCENE ");
  }

  @Test
  public void shouldSortByReverseDocScore() throws Exception {

    db.command("create index Author.ft on Author (name,score) FULLTEXT ENGINE LUCENE ");

    OResultSet resultSet =
        db.query(
            "SELECT score, name from Author where SEARCH_CLASS('*:* ', {"
                + "sort: [ { reverse:true, type:'DOC' }]"
                + "} ) = true ");

    List<Integer> scores =
        resultSet.stream().map(o -> o.<Integer>getProperty("score")).collect(Collectors.toList());

    assertThat(scores).containsExactly(4, 5, 10, 10, 7);
    resultSet.close();
  }

  @Test
  public void shouldSortByReverseScoreFieldValue() throws Exception {

    db.command("create index Author.ft on Author (score) FULLTEXT ENGINE LUCENE ");

    OResultSet resultSet =
        db.query(
            "SELECT score, name from Author where SEARCH_CLASS('*:* ', {"
                + "sort: [ { 'field': 'score', reverse:true, type:'INT' }]"
                + "} ) = true ");

    List<Integer> scores =
        resultSet.stream().map(o -> o.<Integer>getProperty("score")).collect(Collectors.toList());

    assertThat(scores).containsExactly(10, 10, 7, 5, 4);
    resultSet.close();
  }

  @Test
  public void shouldSortByReverseNameValue() throws Exception {

    db.command("create index Author.ft on Author (name) FULLTEXT ENGINE LUCENE ");

    OResultSet resultSet =
        db.query(
            "SELECT score, name from Author where SEARCH_CLASS('*:* ', {"
                + "sort: [ {field: 'name', type:'STRING' , reverse:true}] "
                + "} ) = true ");

    List<String> names =
        resultSet.stream().map(o -> o.<String>getProperty("name")).collect(Collectors.toList());

    assertThat(names)
        .containsExactly(
            "Lennon McCartney", "Jack Mountain", "Grateful Dead", "Chuck Berry", "Bob Dylan");
    resultSet.close();
  }

  @Test
  public void shouldSortByReverseNameValueWithTxRollback() throws Exception {

    db.command("create index Author.ft on Author (name) FULLTEXT ENGINE LUCENE ");

    db.begin();

    OVertex artist = db.newVertex("Author");

    artist.setProperty("name", "Jimi Hendrix");

    db.save(artist);

    OResultSet resultSet =
        db.query(
            "SELECT score, name from Author where SEARCH_CLASS('*:* ', {"
                + "sort: [ {field: 'name', type:'STRING' , reverse:true}] "
                + "} ) = true ");

    List<String> names =
        resultSet.stream().map(o -> o.<String>getProperty("name")).collect(Collectors.toList());

    assertThat(names)
        .containsExactly(
            "Lennon McCartney",
            "Jimi Hendrix",
            "Jack Mountain",
            "Grateful Dead",
            "Chuck Berry",
            "Bob Dylan");

    db.rollback();

    resultSet.close();
    resultSet =
        db.query(
            "SELECT score, name from Author where SEARCH_CLASS('*:* ', {"
                + "sort: [ {field: 'name', type:'STRING' , reverse:true}] "
                + "} ) = true ");

    names = resultSet.stream().map(o -> o.<String>getProperty("name")).collect(Collectors.toList());

    assertThat(names)
        .containsExactly(
            "Lennon McCartney", "Jack Mountain", "Grateful Dead", "Chuck Berry", "Bob Dylan");
    resultSet.close();
  }

  @Test
  public void shouldSortByReverseScoreFieldValueAndThenReverseName() throws Exception {

    db.command("create index Author.ft on Author (name,score) FULLTEXT ENGINE LUCENE ");

    OResultSet resultSet =
        db.query(
            "SELECT score, name from Author where SEARCH_CLASS('*:* ', {"
                + "sort: [ { 'field': 'score', reverse:true, type:'INT' },{field: 'name', type:'STRING' , reverse:true}] "
                + "} ) = true ");

    List<String> names =
        resultSet.stream()
            .map(o -> "" + o.<Integer>getProperty("score") + o.<String>getProperty("name"))
            .collect(Collectors.toList());

    assertThat(names)
        .containsExactly(
            "10Chuck Berry",
            "10Bob Dylan",
            "7Lennon McCartney",
            "5Grateful Dead",
            "4Jack Mountain");
    resultSet.close();
  }
}
