package com.orientechnologies.lucene.functions;

import com.orientechnologies.lucene.test.BaseLuceneTest;
import com.orientechnologies.orient.core.id.ORecordId;
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

    db.command("create index Song.title on Song (title) FULLTEXT ENGINE LUCENE ");

  }

  @Test
  public void shouldSearchMoreLikeThisWithRid() throws Exception {

//    db.query("select from Song").stream().forEach(e-> System.out.println("e = " + e.toElement().toJSON()));

    OResultSet resultSet = db
        .query("SELECT from Song where SEARCH_More([#25:2, #25:3] ) = true");

    assertThat(resultSet).hasSize(48);

    resultSet.close();
  }

  @Test
  @Ignore
  public void shouldSearchMoreLikeThisWithInnerQuery() throws Exception {

//    db.query("select from Song").stream().forEach(e-> System.out.println("e = " + e.toElement().toJSON()));

    OResultSet resultSet = db
        .query("SELECT from Song where SEARCH_More( (LET SELECT @RID FROM SONG WHERE AUTHOR = \"hunter\" ) ) = true");

    assertThat(resultSet).hasSize(2);

    resultSet.close();
  }

}
