package com.orientechnologies.lucene.functions;

import com.orientechnologies.lucene.test.BaseLuceneTest;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Before;
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

    db.command(new OCommandScript("sql", getScriptFromStream(stream))).execute();
//    db.execute("sql", getScriptFromStream(stream));

    db.command("create index Song.title on Song (title) FULLTEXT ENGINE LUCENE METADATA {\"analyzer\":\"" + StandardAnalyzer.class
        .getName() + "\"}");
    db.command("create index Song.author on Song (author) FULLTEXT ENGINE LUCENE METADATA {\"analyzer\":\"" + StandardAnalyzer.class
        .getName() + "\"}");

  }

  @Test
  public void shouldSearchOnIndex() throws Exception {


    //TODO: metadata still not used
    OResultSet resultSet = db.query("SELECT from Song where SEARCH_INDEX('Song.title', 'BELIEVE', {'allowLeadingWildcard': true}) = true");

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
        .query("SELECT from Song where SEARCH_INDEX('Song.title', 'tambourine') = true AND SEARCH_INDEX('Song.author', 'Bob') = true ");

    assertThat(resultSet).hasSize(1);
    resultSet.close();

  }
}
