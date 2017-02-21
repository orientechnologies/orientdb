package com.orientechnologies.lucene.functions;

import com.orientechnologies.lucene.test.BaseLuceneTest;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by frank on 15/01/2017.
 */
public class OLuceneSearchOnClassFunctionTest extends BaseLuceneTest {

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
  @Ignore
  public void shouldSearchOnSingleFieldWithLeadingWildcard() throws Exception {

    //TODO: metadata still not used
    OResultSet resultSet = db
        .query("SELECT from Song where SEARCH_CLASS( '*EVE*', {'allowLeadingWildcard': true}) = true");

//    resultSet.getExecutionPlan().ifPresent(x -> System.out.println(x.prettyPrint(0, 2)));
    assertThat(resultSet).hasSize(2);

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
            "SELECT from Song where SEARCH_CLASS('GOODNIGHT') = true AND SEARCH_CLASS( 'Irene') = true ");

    assertThat(resultSet).hasSize(1);
    resultSet.close();

  }

  @Test
  public void shouldFindNothingWithWrongClass() throws Exception {

    OResultSet resultSet = db
        .query(
            "SELECT from Author where SEARCH_CLASS('(description:happiness) (lyrics:sad)  ') = true ");

    assertThat(resultSet).hasSize(0);
    resultSet.close();

  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowExceptionIfMoreIndexesAreDefined() {

    db.command("create index Song.author on Song (author) FULLTEXT ENGINE LUCENE ");

    OResultSet resultSet = db
        .query(
            "SELECT from Song where SEARCH_CLASS('not important, will fail') = true ");

  }
}
