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

package com.orientechnologies.lucene.test;

import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by Enrico Risa on 07/07/15.
 */
public class LuceneCreateJavaApiTest extends BaseLuceneTest {
  public static final String SONG_CLASS = "Song";

  @Before
  public void init() {
    final OSchema schema = db.getMetadata().getSchema();
    final OClass v = schema.getClass("V");
    final OClass song = schema.createClass(SONG_CLASS);
    song.setSuperClass(v);
    song.createProperty("title", OType.STRING);
    song.createProperty("author", OType.STRING);
    song.createProperty("description", OType.STRING);

    song.createProperty(OType.EMBEDDEDMAP.getName(), OType.EMBEDDEDMAP, OType.STRING);
    song.createProperty(OType.EMBEDDED.getName(), OType.EMBEDDED);
  }

  @Test
  public void testCreateIndex() {
    final OSchema schema = db.getMetadata().getSchema();
    final OClass song = schema.getClass(SONG_CLASS);

    final ODocument meta = new ODocument().field("analyzer", StandardAnalyzer.class.getName());
    final OIndex lucene = song
        .createIndex("Song.title", OClass.INDEX_TYPE.FULLTEXT.toString(), null, meta,
            "LUCENE", new String[] { "title" });

    assertThat(lucene).isNotNull();
    assertThat(lucene.getMetadata().containsField("analyzer")).isTrue();
    assertThat(lucene.getMetadata().<Object>field("analyzer")).isEqualTo(StandardAnalyzer.class.getName());
  }

  @Test
  public void testCreateIndexCompositeWithDefaultAnalyzer() {
    final OSchema schema = db.getMetadata().getSchema();
    final OClass song = schema.getClass(SONG_CLASS);
    final OIndex lucene = song.createIndex("Song.author_description", OClass.INDEX_TYPE.FULLTEXT.toString(),
        null, null, "LUCENE", new String[] { "author", "description" });

    assertThat(lucene).isNotNull();
    assertThat(lucene.getMetadata().containsField("analyzer")).isTrue();
    assertThat(lucene.getMetadata().<Object>field("analyzer")).isEqualTo(StandardAnalyzer.class.getName());
  }

  @Test (expected = UnsupportedOperationException.class)
  public void testCreateIndexWithUnsupportedEmbeddedMap() {
    db.save(new ODocument(SONG_CLASS).fromJSON("{\n" +
        "    \"description\": \"Capital\",\n" +
        "    \"" + OType.EMBEDDEDMAP.getName() + "\": {\n" +
        "    \"text\": \"Hello Rome how are you today?\",\n" +
        "    \"text2\": \"Hello Rome how are you today?\",\n" +
        "    }\n" +
        "}"));
    final OSchema schema = db.getMetadata().getSchema();
    final OClass song = schema.getClass(SONG_CLASS);
    song.createIndex(SONG_CLASS + "." + OType.EMBEDDEDMAP.getName(), OClass.INDEX_TYPE.FULLTEXT.toString(),
        null, null, "LUCENE", new String[] {"description", OType.EMBEDDEDMAP.getName()});
    Assert.assertEquals(1, song.getIndexes().size());
    /*long count = db.query(new OSQLSynchQuery<ODocument>("select from " + SONG_CLASS +
        " where SEARCH_CLASS('capital', {\n" +
        "    \"allowLeadingWildcard\": true ,\n" +
        "    \"lowercaseExpandedTerms\": true\n" +
        "}) = true")).stream().count();
    Assert.assertEquals(1, count);*/

    long count = db.query(new OSQLSynchQuery<ODocument>("select from " + SONG_CLASS +
        " where SEARCH_CLASS('Rome', {\n" +
        "    \"allowLeadingWildcard\": true ,\n" +
        "    \"lowercaseExpandedTerms\": true\n" +
        "}) = true")).stream().count();
    Assert.assertEquals(0, count);
  }

  @Test (expected = UnsupportedOperationException.class)
  public void testCreateIndexWithUnsupportedEmbedded() {
    final OSchema schema = db.getMetadata().getSchema();
    final OClass song = schema.getClass(SONG_CLASS);
    song.createIndex(SONG_CLASS + "." + OType.EMBEDDED.getName(), OClass.INDEX_TYPE.FULLTEXT.toString(),
        null, null, "LUCENE", new String[] {"description", OType.EMBEDDED.getName()});
    Assert.assertEquals(1, song.getIndexes().size());
  }

  /*@Test // (expected = UnsupportedOperationException.class)
  public void testCreateIndexWithUnsupportedEmbeddedMapActualApi() {
    final OSchema schema = db.getMetadata().getSchema();
    final OClass song = schema.getClass(SONG_CLASS);
    song.createIndex(SONG_CLASS + "." + OType.EMBEDDEDMAP.getName(), OClass.INDEX_TYPE.FULLTEXT.toString(),
        null, null, "LUCENE", new String[] {"description", OType.EMBEDDEDMAP.getName()});
    Assert.assertEquals(1, song.getIndexes().size());

    final Map<String, String> entries = new HashMap<String, String>();
    entries.put("text","Hello Rome how are you today?");
    entries.put("text2","Hello Rome how are you today?");

    ODocument doc = new ODocument(SONG_CLASS);
    doc.field(OType.EMBEDDEDMAP.getName(), entries, OType.EMBEDDEDMAP);
    doc.save();

    long count = db.query(new OSQLSynchQuery<ODocument>("select from " + SONG_CLASS +
        " where SEARCH_CLASS('Rome', {\n" +
        "    \"allowLeadingWildcard\": true ,\n" +
        "    \"lowercaseExpandedTerms\": true\n" +
        "}) = true")).stream().count();
    Assert.assertEquals(0, count);
  }*/
}
