package com.orientechnologies.lucene.functions;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.lucene.test.BaseLuceneTest;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

/** Created by frank on 15/01/2017. */
public class OLuceneSearchOnFieldsFunctionTest extends BaseLuceneTest {
  @Before
  public void setUp() throws Exception {
    final InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");
    db.execute("sql", getScriptFromStream(stream));
    db.command("create index Song.title on Song (title) FULLTEXT ENGINE LUCENE ");
    db.command("create index Song.author on Song (author) FULLTEXT ENGINE LUCENE ");
    db.command(
        "create index Song.lyrics_description on Song (lyrics,description) FULLTEXT ENGINE LUCENE ");
  }

  @Test
  public void shouldSearchOnSingleField() throws Exception {
    final OResultSet resultSet =
        db.query("SELECT from Song where SEARCH_FIELDS(['title'], 'BELIEVE') = true");
    assertThat(resultSet).hasSize(2);
    resultSet.close();
  }

  @Test
  public void shouldSearchOnSingleFieldWithLeadingWildcard() throws Exception {
    // TODO: metadata still not used
    final OResultSet resultSet =
        db.query(
            "SELECT from Song where SEARCH_FIELDS(['title'], '*EVE*', {'allowLeadingWildcard': true}) = true");
    assertThat(resultSet).hasSize(14);
    resultSet.close();
  }

  @Test
  public void shouldSearhOnTwoFieldsInOR() throws Exception {
    final OResultSet resultSet =
        db.query(
            "SELECT from Song where SEARCH_FIELDS(['title'], 'BELIEVE') = true OR SEARCH_FIELDS(['author'], 'Bob') = true ");
    assertThat(resultSet).hasSize(41);
    resultSet.close();
  }

  @Test
  public void shouldSearchOnTwoFieldsInAND() throws Exception {
    final OResultSet resultSet =
        db.query(
            "SELECT from Song where SEARCH_FIELDS(['title'], 'tambourine') = true AND SEARCH_FIELDS(['author'], 'Bob') = true ");
    assertThat(resultSet).hasSize(1);
    resultSet.close();
  }

  @Test
  public void shouldSearhOnTwoFieldsWithLeadingWildcardInAND() throws Exception {
    final OResultSet resultSet =
        db.query(
            "SELECT from Song where SEARCH_FIELDS(['title'], 'tambourine') = true AND SEARCH_FIELDS(['author'], 'Bob', {'allowLeadingWildcard': true}) = true ");
    assertThat(resultSet).hasSize(1);
    resultSet.close();
  }

  @Test
  public void shouldSearchOnMultiFieldIndex() throws Exception {
    OResultSet resultSet =
        db.query(
            "SELECT from Song where SEARCH_FIELDS(['lyrics','description'], '(description:happiness) (lyrics:sad)  ') = true ");
    assertThat(resultSet).hasSize(2);
    resultSet.close();

    resultSet =
        db.query(
            "SELECT from Song where SEARCH_FIELDS(['description','lyrics'], '(description:happiness) (lyrics:sad)  ') = true ");

    assertThat(resultSet).hasSize(2);
    resultSet.close();

    resultSet =
        db.query(
            "SELECT from Song where SEARCH_FIELDS(['description'], '(description:happiness) (lyrics:sad)  ') = true ");
    assertThat(resultSet).hasSize(2);
    resultSet.close();
  }

  @Test(expected = OCommandExecutionException.class)
  public void shouldFailWithWrongFieldName() throws Exception {
    db.query(
        "SELECT from Song where SEARCH_FIELDS(['wrongName'], '(description:happiness) (lyrics:sad)  ') = true ");
  }

  @Test
  public void shouldSearchWithHesitance() throws Exception {
    db.command("create class RockSong extends Song");
    db.command("create vertex RockSong set title=\"This is only rock\", author=\"A cool rocker\"");
    final OResultSet resultSet =
        db.query("SELECT from RockSong where SEARCH_FIELDS(['title'], '+only +rock') = true ");
    assertThat(resultSet).hasSize(1);
    resultSet.close();
  }

  @Test
  public void testSquareBrackets() throws Exception {
    final String className = "testSquareBrackets";
    final String classNameE = "testSquareBracketsE";

    db.command("create class " + className + " extends V;");
    db.command("create property " + className + ".id Integer;");
    db.command("create property " + className + ".name String;");
    db.command(
        "CREATE INDEX " + className + ".name ON " + className + "(name) FULLTEXT ENGINE LUCENE;");

    db.command("CREATE CLASS " + classNameE + " EXTENDS E");

    db.command("insert into " + className + " set id = 1, name = 'A';");
    db.command("insert into " + className + " set id = 2, name = 'AB';");
    db.command("insert into " + className + " set id = 3, name = 'ABC';");
    db.command("insert into " + className + " set id = 4, name = 'ABCD';");

    db.command(
        "CREATE EDGE "
            + classNameE
            + " FROM (SELECT FROM "
            + className
            + " WHERE id = 1) to (SELECT FROM "
            + className
            + " WHERE id IN [2, 3, 4]);");

    final OResultSet result =
        db.query(
            "SELECT out('"
                + classNameE
                + "')[SEARCH_FIELDS(['name'], 'A*') = true] as theList FROM "
                + className
                + " WHERE id = 1;");
    assertThat(result.hasNext());
    final OResult item = result.next();
    assertThat((Object) item.getProperty("theList")).isInstanceOf(List.class);
    assertThat((List) item.getProperty("theList")).hasSize(3);
    result.close();
  }

  @Test
  public void shouldSupportParameterizedMetadata() throws Exception {
    final String query = "SELECT from Song where SEARCH_FIELDS(['title'], '*EVE*', ?) = true";

    db.query(query, "{'allowLeadingWildcard': true}").close();
    db.query(query, new ODocument("allowLeadingWildcard", Boolean.TRUE)).close();

    Map<String, Object> mdMap = new HashMap();
    mdMap.put("allowLeadingWildcard", true);
    db.query(query, new Object[] {mdMap}).close();
  }
}
