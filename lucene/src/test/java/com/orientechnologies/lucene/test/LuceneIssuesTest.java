package com.orientechnologies.lucene.test;

import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.io.InputStream;
import java.util.List;

/**
 * Created by frank on 27/04/2017.
 */
public class LuceneIssuesTest extends BaseLuceneTest {

  @Test
  public void testGh_7382() throws Exception {

    InputStream stream = ClassLoader.getSystemResourceAsStream("testGh_7382.osql");

    db.command(new OCommandScript("sql", getScriptFromStream(stream))).execute();

    List<ODocument> results = db.query(new OSQLSynchQuery<ODocument>(
        "select server,date from index:class_7382_multi WHERE key = 'server:206012226875414 AND date:[201703120000 TO  201703120001]' "));

    Assertions.assertThat(results).hasSize(1);

  }

}
