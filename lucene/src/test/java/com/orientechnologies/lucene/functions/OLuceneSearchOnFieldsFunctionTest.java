package com.orientechnologies.lucene.functions;

import com.orientechnologies.lucene.test.BaseLuceneTest;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by frank on 15/01/2017.
 */
public class OLuceneSearchOnFieldsFunctionTest extends BaseLuceneTest {

  @Before
  public void setUp() throws Exception {
    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");

    db.execute("sql", getScriptFromStream(stream));

    db.command("create index Song.title on Song (title) FULLTEXT ENGINE LUCENE ");
    db.command("create index Song.author on Song (author) FULLTEXT ENGINE LUCENE ");

    db.command("create index Song.lyrics_description on Song (lyrics,description) FULLTEXT ENGINE LUCENE ");

  }

  @Test
  public void shouldSearchOnSingleField() throws Exception {

    OResultSet resultSet = db
        .query("SELECT from Song where SEARCH_FIELDS(['title'], 'BELIEVE') = true");

    assertThat(resultSet).hasSize(2);

    resultSet.close();
  }

  @Test
  public void shouldSearchOnSingleFieldWithLeadingWildcard() throws Exception {

    //TODO: metadata still not used
    OResultSet resultSet = db
        .query("SELECT from Song where SEARCH_INDEX('Song.title', '*EVE*', {'allowLeadingWildcard': true}) = true");

    assertThat(resultSet).hasSize(14);

    resultSet.close();
  }

  @Test
  public void shouldSearhOnTwoFieldsInOR() throws Exception {

    OResultSet resultSet = db
        .query("SELECT from Song where SEARCH_FIELDS(['title'], 'BELIEVE') = true OR SEARCH_FIELDS(['author'], 'Bob') = true ");

    assertThat(resultSet).hasSize(41);
    resultSet.close();

  }

  @Test
  public void shouldSearhOnTwoFieldsInAND() throws Exception {

    OResultSet resultSet = db
        .query(
            "SELECT from Song where SEARCH_FIELDS(['title'], 'tambourine') = true AND SEARCH_FIELDS(['author'], 'Bob') = true ");

    assertThat(resultSet).hasSize(1);
    resultSet.close();

  }

  @Test
  public void shouldSearhOnTwoFieldsWithLeadingWildcardInAND() throws Exception {

    OResultSet resultSet = db
        .query(
            "SELECT from Song where SEARCH_FIELDS(['title'], 'tambourine') = true AND SEARCH_FIELDS(['author'], 'Bob', {'allowLeadingWildcard': true}) = true ");

    assertThat(resultSet).hasSize(1);
    resultSet.close();

  }

  @Test
  public void shouldSearchOnMultiFieldIndex() throws Exception {

    OResultSet resultSet = db
        .query(
            "SELECT from Song where SEARCH_FIELDS(['lyrics','description'], '(description:happiness) (lyrics:sad)  ') = true ");

    assertThat(resultSet).hasSize(2);
    resultSet.close();

    resultSet = db
        .query(
            "SELECT from Song where SEARCH_FIELDS(['description','lyrics'], '(description:happiness) (lyrics:sad)  ') = true ");

    assertThat(resultSet).hasSize(2);
    resultSet.close();

    resultSet = db
        .query(
            "SELECT from Song where SEARCH_FIELDS(['description'], '(description:happiness) (lyrics:sad)  ') = true ");

    assertThat(resultSet).hasSize(2);
    resultSet.close();

  }

  @Test(expected = OCommandExecutionException.class)
  public void shouldFailWithWrongFieldName() throws Exception {

    db.query("SELECT from Song where SEARCH_FIELDS(['wrongName'], '(description:happiness) (lyrics:sad)  ') = true ");

  }

  @Test
  public void shouldSearchWithHesitance() throws Exception {

    db.command("create class RockSong extends Song");
    db.command("create vertex RockSong set title=\"This is only rock\", author=\"A cool rocker\"");

    OResultSet resultSet = db
        .query(
            "SELECT from RockSong where SEARCH_FIELDS(['title'], '+only +rock') = true ");

    assertThat(resultSet).hasSize(1);
    resultSet.close();

  }
}
