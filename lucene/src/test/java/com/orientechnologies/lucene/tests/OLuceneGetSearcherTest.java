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

import com.orientechnologies.lucene.index.OLuceneIndexNotUnique;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Created by Enrico Risa on 29/04/15. */
public class OLuceneGetSearcherTest extends OLuceneBaseTest {

  @Before
  public void init() {
    OClass song = db.createVertexClass("Person");
    song.createProperty("isDeleted", OType.BOOLEAN);

    db.command("create index Person.isDeleted on Person (isDeleted) FULLTEXT ENGINE LUCENE");
  }

  @Test
  public void testSearcherInstance() {

    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "Person.isDeleted");

    Assert.assertEquals(true, index.getInternal() instanceof OLuceneIndexNotUnique);

    OLuceneIndexNotUnique idx = (OLuceneIndexNotUnique) index.getInternal();

    Assert.assertNotNull(idx.searcher());
  }
}
