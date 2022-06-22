package com.orientechnologies.lucene.functions;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.lucene.test.BaseLuceneTest;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

/** Created by frank on 15/01/2017. */
public class OLuceneSearchOnIndexFunctionTest extends BaseLuceneTest {

  @Before
  public void setUp() throws Exception {
    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");

    //    db.command(new OCommandScript("sql", getScriptFromStream(stream))).execute();
    db.execute("sql", getScriptFromStream(stream));

    db.command("create index Song.title on Song (title) FULLTEXT ENGINE LUCENE ");
    db.command("create index Song.author on Song (author) FULLTEXT ENGINE LUCENE ");
    db.command("create index Author.name on Author (name) FULLTEXT ENGINE LUCENE ");
    db.command(
        "create index Song.lyrics_description on Song (lyrics,description) FULLTEXT ENGINE LUCENE ");
  }

  @Test
  public void shouldSearchOnSingleIndex() throws Exception {

    OResultSet resultSet =
        db.query("SELECT from Song where SEARCH_INDEX('Song.title', 'BELIEVE') = true");

    //    resultSet.getExecutionPlan().ifPresent(x -> System.out.println(x.prettyPrint(0, 2)));
    assertThat(resultSet).hasSize(2);

    resultSet.close();

    resultSet = db.query("SELECT from Song where SEARCH_INDEX('Song.title', \"bel*\") = true");

    assertThat(resultSet).hasSize(3);
    resultSet.close();

    resultSet = db.query("SELECT from Song where SEARCH_INDEX('Song.title', 'bel*') = true");

    assertThat(resultSet).hasSize(3);

    resultSet.close();
  }

  @Test
  public void shouldFindNothingOnEmptyQuery() throws Exception {

    OResultSet resultSet = db.query("SELECT from Song where SEARCH_INDEX('Song.title', '') = true");

    //    resultSet.getExecutionPlan().ifPresent(x -> System.out.println(x.prettyPrint(0, 2)));
    assertThat(resultSet).hasSize(0);

    resultSet.close();
  }

  @Test
  //  @Ignore
  public void shouldSearchOnSingleIndexWithLeadingWildcard() throws Exception {

    // TODO: metadata still not used
    OResultSet resultSet =
        db.query(
            "SELECT from Song where SEARCH_INDEX('Song.title', '*EVE*', {'allowLeadingWildcard': true}) = true");

    //    resultSet.getExecutionPlan().ifPresent(x -> System.out.println(x.prettyPrint(0, 2)));
    assertThat(resultSet).hasSize(14);

    resultSet.close();
  }

  @Test
  public void shouldSearchOnTwoIndexesInOR() throws Exception {

    OResultSet resultSet =
        db.query(
            "SELECT from Song where SEARCH_INDEX('Song.title', 'BELIEVE') = true OR SEARCH_INDEX('Song.author', 'Bob') = true ");

    assertThat(resultSet).hasSize(41);
    resultSet.close();
  }

  @Test
  public void shouldSearchOnTwoIndexesInAND() throws Exception {

    OResultSet resultSet =
        db.query(
            "SELECT from Song where SEARCH_INDEX('Song.title', 'tambourine') = true AND SEARCH_INDEX('Song.author', 'Bob') = true ");

    assertThat(resultSet).hasSize(1);
    resultSet.close();
  }

  @Test
  public void shouldSearchOnTwoIndexesWithLeadingWildcardInAND() throws Exception {

    OResultSet resultSet =
        db.query(
            "SELECT from Song where SEARCH_INDEX('Song.title', 'tambourine') = true AND SEARCH_INDEX('Song.author', 'Bob', {'allowLeadingWildcard': true}) = true ");

    assertThat(resultSet).hasSize(1);
    resultSet.close();
  }

  @Test(expected = OCommandExecutionException.class)
  public void shouldFailWithWrongIndexName() throws Exception {

    db.query("SELECT from Song where SEARCH_INDEX('Song.wrongName', 'tambourine') = true ").close();
  }

  @Test
  public void shouldSupportParameterizedMetadata() throws Exception {
    final String query = "SELECT from Song where SEARCH_INDEX('Song.title', '*EVE*', ?) = true";

    db.query(query, "{'allowLeadingWildcard': true}").close();
    db.query(query, new ODocument("allowLeadingWildcard", Boolean.TRUE)).close();

    Map<String, Object> mdMap = new HashMap();
    mdMap.put("allowLeadingWildcard", true);
    db.query(query, new Object[] {mdMap}).close();
  }
}
