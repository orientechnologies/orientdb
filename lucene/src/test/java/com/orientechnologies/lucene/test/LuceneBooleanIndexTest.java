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

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.List;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Created by Enrico Risa on 29/04/15. */
public class LuceneBooleanIndexTest extends BaseLuceneTest {

  @Before
  public void init() {
    OSchema schema = db.getMetadata().getSchema();
    OClass v = schema.getClass("V");
    OClass song = schema.createClass("Person");
    song.setSuperClass(v);
    song.createProperty("isDeleted", OType.BOOLEAN);

    db.command(
            new OCommandSQL(
                "create index Person.isDeleted on Person (isDeleted) FULLTEXT ENGINE LUCENE"))
        .execute();
  }

  @Test
  public void insertPerson() {

    for (int i = 0; i < 1000; i++) {
      ODocument doc = new ODocument("Person");
      doc.field("isDeleted", i % 2 == 0);
      db.save(doc);
    }

    List<ODocument> docs =
        db.query(new OSQLSynchQuery<ODocument>("select from Person where isDeleted lucene false"));

    Assert.assertEquals(500, docs.size());
    Assert.assertEquals(false, docs.get(0).field("isDeleted"));
    docs =
        db.query(new OSQLSynchQuery<ODocument>("select from Person where isDeleted lucene true"));

    Assert.assertEquals(500, docs.size());
    Assert.assertEquals(true, docs.get(0).field("isDeleted"));
  }

  @Test
  public void testMemoryIndex() throws ParseException {
    // TODO To be used in evaluate Record
    MemoryIndex index = new MemoryIndex();

    Document doc = new Document();
    doc.add(new StringField("text", "my text", Field.Store.YES));
    StandardAnalyzer analyzer = new StandardAnalyzer();

    for (IndexableField field : doc.getFields()) {
      index.addField(field.name(), field.stringValue(), analyzer);
    }

    QueryParser parser = new QueryParser("text", analyzer);
    float score = index.search(parser.parse("+text:my"));
  }
}
