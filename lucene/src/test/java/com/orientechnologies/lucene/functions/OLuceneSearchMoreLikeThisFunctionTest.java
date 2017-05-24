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
public class OLuceneSearchMoreLikeThisFunctionTest extends BaseLuceneTest {

  @Before
  public void setUp() throws Exception {
    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");

    db.execute("sql", getScriptFromStream(stream));

  }

  @Test
  public void shouldSearchMoreLikeThisWithRid() throws Exception {

    db.command("create index Song.title on Song (title) FULLTEXT ENGINE LUCENE ");

    OResultSet resultSet = db
        .query("SELECT from Song where SEARCH_More([#25:2, #25:3],{'minTermFreq':1, 'minDocFreq':1} ) = true");

    assertThat(resultSet).hasSize(48);

    resultSet.close();
  }

  @Test
  public void shouldSearchMoreLikeThisWithRidOnMultiFieldsIndex() throws Exception {

    db.command("create index Song.multi on Song (title,author) FULLTEXT ENGINE LUCENE ");

    OResultSet resultSet = db
        .query("SELECT from Song where SEARCH_More([#25:2, #25:3] , {'minTermFreq':1, 'minDocFreq':1} ) = true");

    assertThat(resultSet).hasSize(84);

    resultSet.close();
  }

  @Test
  public void shouldSearchOnFieldAndMoreLikeThisWithRidOnMultiFieldsIndex() throws Exception {

    db.command("create index Song.multi on Song (title) FULLTEXT ENGINE LUCENE ");

    OResultSet resultSet = db
        .query(
            "SELECT from Song where author ='Hunter' AND SEARCH_More([#25:2, #25:3,#25:4,#25:5],{'minTermFreq':1, 'minDocFreq':1} ) = true");

    assertThat(resultSet).hasSize(8);

    resultSet.close();

//    resultSet = db
//        .query("SELECT from Song where SEARCH_More([#25:2], {'minTermFreq':1, 'minDocFreq':1} ) = true OR author ='Hunter' ");
//    System.out.println(resultSet.getExecutionPlan().get().prettyPrint(1, 1));
//    assertThat(resultSet).hasSize(84);
//
//    resultSet.close();
  }

  @Test
  public void shouldSearchMoreLikeThisWithRidOnMultiFieldsIndexWithMetadata() throws Exception {

    db.command("create index Song.multi on Song (title,author) FULLTEXT ENGINE LUCENE ");

    OResultSet resultSet = db
        .query(
            "SELECT from Song where SEARCH_More( [#25:2, #25:3] , {'fields': [ 'title' ], 'minTermFreq':1, 'minDocFreq':1}) = true");

    System.out.println(resultSet.getExecutionPlan().get().prettyPrint(1, 1));
    assertThat(resultSet).hasSize(84);

    resultSet.close();
  }

  @Test
  @Ignore
  public void shouldSearchMoreLikeThisWithInnerQuery() throws Exception {

//    db.query("SELECT @RID FROM Song WHERE author = 'Hunter' ").stream().forEach(e-> System.out.println("e = " + e.toElement().toJSON()));

    OResultSet resultSet = db
        .query("SELECT from Song   where SEARCH_More( (SELECT @rid FROM Song WHERE author = 'Hunter') ) = true");

    assertThat(resultSet).hasSize(2);

    resultSet.close();
  }

}
