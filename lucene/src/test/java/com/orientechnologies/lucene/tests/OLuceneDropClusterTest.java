/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.lucene.tests;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import java.io.InputStream;
import java.util.logging.Level;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.assertj.core.api.Assertions;
import org.junit.Test;

/** Created by enricorisa on 26/09/14. */
public class OLuceneDropClusterTest extends OLuceneBaseTest {

  @Test
  public void shouldRemoveCluster() throws Exception {
    OLogManager.instance().setConsoleLevel(Level.FINE.getName());
    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");

    db.execute("sql", getScriptFromStream(stream));

    db.command(
        "create index Song.title on Song (title) FULLTEXT ENGINE LUCENE METADATA {\"default\":\""
            + StandardAnalyzer.class.getName()
            + "\"}");
    db.command(
        "create index Song.author on Song (author) FULLTEXT ENGINE LUCENE METADATA {\"default\":\""
            + StandardAnalyzer.class.getName()
            + "\"}");

    OMetadataInternal metadata = db.getMetadata();

    long initialIndexSize =
        metadata.getIndexManagerInternal().getIndex(db, "Song.title").getInternal().size();

    int[] clusterIds = metadata.getSchema().getClass("Song").getClusterIds();

    db.dropCluster(clusterIds[1]);

    long afterDropIndexSize =
        metadata.getIndexManagerInternal().getIndex(db, "Song.title").getInternal().size();

    Assertions.assertThat(afterDropIndexSize).isLessThan(initialIndexSize);
  }
}
