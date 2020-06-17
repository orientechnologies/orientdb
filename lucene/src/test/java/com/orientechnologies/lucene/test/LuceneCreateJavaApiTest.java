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

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Created by Enrico Risa on 07/07/15. */
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
  }

  @Test
  public void testCreateIndex() {
    final OSchema schema = db.getMetadata().getSchema();
    final OClass song = schema.getClass(SONG_CLASS);

    final ODocument meta = new ODocument().field("analyzer", StandardAnalyzer.class.getName());
    final OIndex lucene =
        song.createIndex(
            "Song.title",
            OClass.INDEX_TYPE.FULLTEXT.toString(),
            null,
            meta,
            "LUCENE",
            new String[] {"title"});

    assertThat(lucene).isNotNull();
    assertThat(lucene.getMetadata().containsField("analyzer")).isTrue();
    assertThat(lucene.getMetadata().<Object>field("analyzer"))
        .isEqualTo(StandardAnalyzer.class.getName());
  }

  @Test
  public void testCreateIndexCompositeWithDefaultAnalyzer() {
    final OSchema schema = db.getMetadata().getSchema();
    final OClass song = schema.getClass(SONG_CLASS);
    final OIndex lucene =
        song.createIndex(
            "Song.author_description",
            OClass.INDEX_TYPE.FULLTEXT.toString(),
            null,
            null,
            "LUCENE",
            new String[] {"author", "description"});

    assertThat(lucene).isNotNull();
    assertThat(lucene.getMetadata().containsField("analyzer")).isTrue();
    assertThat(lucene.getMetadata().<Object>field("analyzer"))
        .isEqualTo(StandardAnalyzer.class.getName());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testCreateIndexWithUnsupportedEmbedded() {
    final OSchema schema = db.getMetadata().getSchema();
    final OClass song = schema.getClass(SONG_CLASS);
    song.createProperty(OType.EMBEDDED.getName(), OType.EMBEDDED);
    song.createIndex(
        SONG_CLASS + "." + OType.EMBEDDED.getName(),
        OClass.INDEX_TYPE.FULLTEXT.toString(),
        null,
        null,
        "LUCENE",
        new String[] {"description", OType.EMBEDDED.getName()});
    Assert.assertEquals(1, song.getIndexes().size());
  }

  @Test
  public void testCreateIndexEmbeddedMapJSON() {
    db.save(
        new ODocument(SONG_CLASS)
            .fromJSON(
                "{\n"
                    + "    \"description\": \"Capital\",\n"
                    + "    \"String"
                    + OType.EMBEDDEDMAP.getName()
                    + "\": {\n"
                    + "    \"text\": \"Hello Rome how are you today?\",\n"
                    + "    \"text2\": \"Hello Bolzano how are you today?\",\n"
                    + "    }\n"
                    + "}"));
    final OClass song = createEmbeddedMapIndex();
    checkCreatedEmbeddedMapIndex(song, "LUCENE");

    final List<?> result = queryIndexEmbeddedMapClass("Bolzano", 1);
    result.stream()
        .forEach(
            entry -> {
              final OIdentifiable oid = (OIdentifiable) entry;
              System.out.println(oid.toString());
            });
  }

  @Test
  public void testCreateIndexEmbeddedMapApi() {
    addDocumentViaAPI();

    final OClass song = createEmbeddedMapIndex();
    checkCreatedEmbeddedMapIndex(song, "LUCENE");

    final List<?> result = queryIndexEmbeddedMapClass("Bolzano", 1);
    result.stream()
        .forEach(
            entry -> {
              final OIdentifiable oid = (OIdentifiable) entry;
              System.out.println(oid.toString());
            });
  }

  @Test
  public void testCreateIndexEmbeddedMapApiSimpleTree() {
    addDocumentViaAPI();

    final OClass song = createEmbeddedMapIndexSimple();
    checkCreatedEmbeddedMapIndex(song, "CELL_BTREE");

    final List<?> result = queryIndexEmbeddedMapClass("Hello Bolzano how are you today?", 0);
    result.stream()
        .forEach(
            entry -> {
              final OIdentifiable oid = (OIdentifiable) entry;
              System.out.println("result: " + oid.toString());
            });
  }

  private void addDocumentViaAPI() {
    final Map<String, String> entries = new HashMap<>();
    entries.put("text", "Hello Rome how are you today?");
    entries.put("text2", "Hello Bolzano how are you today?");

    final ODocument doc = new ODocument(SONG_CLASS);
    doc.field("description", "Capital", OType.STRING);
    doc.field("String" + OType.EMBEDDEDMAP.getName(), entries, OType.EMBEDDEDMAP, OType.STRING);
    db.save(doc);
  }

  @Test
  public void testCreateIndexEmbeddedMapApiSimpleDoesNotReturnResult() {
    addDocumentViaAPI();

    final OClass song = createEmbeddedMapIndexSimple();
    checkCreatedEmbeddedMapIndex(song, "CELL_BTREE");

    final List<?> result = queryIndexEmbeddedMapClass("Bolzano", 0);
    result.stream()
        .forEach(
            entry -> {
              final OIdentifiable oid = (OIdentifiable) entry;
              System.out.println("result: " + oid.toString());
            });
  }

  private List<?> queryIndexEmbeddedMapClass(final String searchTerm, final int expectedCount) {
    final List<?> result =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select from "
                    + SONG_CLASS
                    + " where SEARCH_CLASS('"
                    + searchTerm
                    + "', {\n"
                    + "    \"allowLeadingWildcard\": true ,\n"
                    + "    \"lowercaseExpandedTerms\": true\n"
                    + "}) = true"));
    Assert.assertEquals(expectedCount, result.stream().count());
    return result;
  }

  private void checkCreatedEmbeddedMapIndex(final OClass clazz, final String expectedAlgorithm) {
    final OIndex index = clazz.getIndexes().iterator().next();
    System.out.println("key-name: " + index.getIndexId() + "-" + index.getName());

    Assert.assertEquals("index algorithm", expectedAlgorithm, index.getAlgorithm());
    Assert.assertEquals("index type", "FULLTEXT", index.getType());
    Assert.assertEquals("Key type", OType.STRING, index.getKeyTypes()[0]);
    Assert.assertEquals(
        "Definition field", "StringEmbeddedMap", index.getDefinition().getFields().get(0));
    Assert.assertEquals(
        "Definition field to index",
        "StringEmbeddedMap by value",
        index.getDefinition().getFieldsToIndex().get(0));
    Assert.assertEquals("Definition type", OType.STRING, index.getDefinition().getTypes()[0]);
  }

  private OClass createEmbeddedMapIndex() {
    final OSchema schema = db.getMetadata().getSchema();
    final OClass song = schema.getClass(SONG_CLASS);
    song.createProperty("String" + OType.EMBEDDEDMAP.getName(), OType.EMBEDDEDMAP, OType.STRING);
    song.createIndex(
        SONG_CLASS + "." + OType.EMBEDDEDMAP.getName(),
        OClass.INDEX_TYPE.FULLTEXT.toString(),
        null,
        null,
        "LUCENE",
        new String[] {"String" + OType.EMBEDDEDMAP.getName() + " by value"});
    Assert.assertEquals(1, song.getIndexes().size());
    return song;
  }

  private OClass createEmbeddedMapIndexSimple() {
    final OSchema schema = db.getMetadata().getSchema();
    final OClass song = schema.getClass(SONG_CLASS);
    song.createProperty("String" + OType.EMBEDDEDMAP.getName(), OType.EMBEDDEDMAP, OType.STRING);
    song.createIndex(
        SONG_CLASS + "." + OType.EMBEDDEDMAP.getName(),
        OClass.INDEX_TYPE.FULLTEXT.toString(),
        new String[] {"String" + OType.EMBEDDEDMAP.getName() + " by value"});
    Assert.assertEquals(1, song.getIndexes().size());
    return song;
  }
}
