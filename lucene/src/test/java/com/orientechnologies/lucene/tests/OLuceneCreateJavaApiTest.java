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

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Before;
import org.junit.Test;

/** Created by Enrico Risa on 07/07/15. */
public class OLuceneCreateJavaApiTest extends OLuceneBaseTest {

  @Before
  public void init() {
    OClass song = db.createVertexClass("Song");
    song.createProperty("title", OType.STRING);
    song.createProperty("author", OType.STRING);
    song.createProperty("description", OType.STRING);
  }

  @Test
  public void testCreateIndex() {
    OSchema schema = db.getMetadata().getSchema();

    OClass song = schema.getClass("Song");

    ODocument meta = new ODocument().field("analyzer", StandardAnalyzer.class.getName());
    OIndex lucene =
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
    OSchema schema = db.getMetadata().getSchema();

    OClass song = schema.getClass("Song");

    OIndex lucene =
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
}
