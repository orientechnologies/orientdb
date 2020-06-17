package com.orientechnologies.lucene.tests;

import static com.orientechnologies.lucene.functions.OLuceneFunctionsUtils.doubleEscape;
import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.io.InputStream;
import org.junit.Before;
import org.junit.Test;

/** Created by frank on 09/05/2017. */
public class OLuceneMetadataFieldsTest extends OLuceneBaseTest {

  @Before
  public void setUp() throws Exception {
    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");

    db.execute("sql", getScriptFromStream(stream));

    db.command("create index Song.title on Song (title) FULLTEXT ENGINE LUCENE ");
  }

  @Test
  public void shouldFetchOnlyFromACluster() throws Exception {

    assertThat(
            db.getMetadata()
                .getIndexManagerInternal()
                .getIndex(db, "Song.title")
                .getInternal()
                .size())
        .isEqualTo(585);

    int cluster = db.getMetadata().getSchema().getClass("Song").getClusterIds()[1];
    db.commit();

    OResultSet results =
        db.query("SELECT FROM Song WHERE search_class('+_CLUSTER:" + cluster + "')=true ");

    assertThat(results).hasSize(73);
    results.close();
  }

  @Test
  public void shouldFetchByRid() throws Exception {

    String ridQuery = doubleEscape("#26:4 #26:5");
    OResultSet results =
        db.query("SELECT FROM Song WHERE search_class('RID:(" + ridQuery + ") ')=true ");

    assertThat(results).hasSize(2);
    results.close();
  }
}
