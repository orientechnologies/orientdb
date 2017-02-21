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
public class OLuceneSearchOnIndexFunctionTest extends BaseLuceneTest {

  @Before
  public void setUp() throws Exception {
    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");

//    db.command(new OCommandScript("sql", getScriptFromStream(stream))).execute();
    db.execute("sql", getScriptFromStream(stream));

    db.command("create index Song.title on Song (title) FULLTEXT ENGINE LUCENE ");
    db.command("create index Song.author on Song (author) FULLTEXT ENGINE LUCENE ");
    db.command("create index Author.name on Author (name) FULLTEXT ENGINE LUCENE ");
    db.command("create index Song.lyrics_description on Song (lyrics,description) FULLTEXT ENGINE LUCENE ");

  }

  @Test
  public void shouldSearchOnSingleIndex() throws Exception {

    OResultSet resultSet = db
        .query("SELECT from Song where SEARCH_INDEX('Song.title', 'BELIEVE') = true");

//    resultSet.getExecutionPlan().ifPresent(x -> System.out.println(x.prettyPrint(0, 2)));
    assertThat(resultSet).hasSize(2);

    resultSet.close();
  }

  @Test
  public void shouldFindEmptyResultOnWrongIndexName() throws Exception {

    //search on Song providing index on Author
    OResultSet resultSet = db
        .query("SELECT from Song where SEARCH_INDEX('Author.name', 'BELIEVE') = true");

    assertThat(resultSet).hasSize(0);

    resultSet.close();
  }

  @Test
  @Ignore
  public void shouldSearchOnSingleIndexWithLeadingWildcard() throws Exception {

    //TODO: metadata still not used
    OResultSet resultSet = db
        .query("SELECT from Song where SEARCH_INDEX('Song.title', '*EVE*', {'allowLeadingWildcard': true}) = true");

//    resultSet.getExecutionPlan().ifPresent(x -> System.out.println(x.prettyPrint(0, 2)));
    assertThat(resultSet).hasSize(2);

    resultSet.close();
  }

  @Test
  public void shouldSearhOnTwoIndexesInOR() throws Exception {

    OResultSet resultSet = db
        .query("SELECT from Song where SEARCH_INDEX('Song.title', 'BELIEVE') = true OR SEARCH_INDEX('Song.author', 'Bob') = true ");

    assertThat(resultSet).hasSize(41);
    resultSet.close();

  }

  @Test
  public void shouldSearhOnTwoIndexesInAND() throws Exception {

    OResultSet resultSet = db
        .query(
            "SELECT from Song where SEARCH_INDEX('Song.title', 'tambourine') = true AND SEARCH_INDEX('Song.author', 'Bob') = true ");

    assertThat(resultSet).hasSize(1);
    resultSet.close();

  }

}
