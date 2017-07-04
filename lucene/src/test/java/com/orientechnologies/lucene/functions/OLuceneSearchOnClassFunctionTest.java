package com.orientechnologies.lucene.functions;

import com.orientechnologies.lucene.tests.OLuceneBaseTest;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by frank on 15/01/2017.
 */
public class OLuceneSearchOnClassFunctionTest extends OLuceneBaseTest {

  @Before
  public void setUp() throws Exception {
    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");

    db.execute("sql", getScriptFromStream(stream));

    db.command("create index Song.title on Song (title) FULLTEXT ENGINE LUCENE ");

  }

  @Test
  public void shouldSearchOnClass() throws Exception {

    OResultSet resultSet = db
        .query("SELECT from Song where SEARCH_Class('BELIEVE') = true");

    assertThat(resultSet).hasSize(2);

    resultSet.close();
  }

  @Test
  public void shouldSearchOnSingleFieldWithLeadingWildcard() throws Exception {

    OResultSet resultSet = db
        .query("SELECT from Song where SEARCH_CLASS( '*EVE*', {'allowLeadingWildcard': true}) = true");

    assertThat(resultSet).hasSize(14);

    resultSet.close();
  }

  @Test
  public void shouldSearchInOr() throws Exception {

    OResultSet resultSet = db
        .query("SELECT from Song where SEARCH_CLASS('BELIEVE') = true OR SEARCH_CLASS('GOODNIGHT') = true ");

    assertThat(resultSet).hasSize(5);
    resultSet.close();

  }

  @Test
  public void shouldSearchInAnd() throws Exception {

    OResultSet resultSet = db
        .query(
            "SELECT from Song where SEARCH_CLASS('GOODNIGHT') = true AND SEARCH_CLASS( 'Irene', {'allowLeadingWildcard': true}) = true ");

    assertThat(resultSet).hasSize(1);
    resultSet.close();

  }

  @Test(expected = OCommandExecutionException.class)
  public void shouldThrowExceptionWithWrongClass() throws Exception {

    OResultSet resultSet = db
        .query(
            "SELECT from Author where SEARCH_CLASS('(description:happiness) (lyrics:sad)  ') = true ");

  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowExceptionIfMoreIndexesAreDefined() {

    db.command("create index Song.author on Song (author) FULLTEXT ENGINE LUCENE ");

    OResultSet resultSet = db
        .query(
            "SELECT from Song where SEARCH_CLASS('not important, will fail') = true ");

  }

  @Test
  public void shouldHighlightTitle() throws Exception {

    OResultSet resultSet = db.query(
        "SELECT title, $title_hl from Song where SEARCH_CLASS('believe', {"
            + "highlight: { fields: ['title'], 'start': '<span>', 'end': '</span>' } }) = true ");

    resultSet.stream()
        .forEach(r -> assertThat(r.<String>getProperty("$title_hl")).containsIgnoringCase("<span>believe</span>"));

  }

}
