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
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by Enrico Risa on 14/08/15.
 */
public class LuceneIndexCreateDropTest extends BaseLuceneTest {

  public LuceneIndexCreateDropTest() {
  }

  @Before
  public void init() {
    OClass type = db.createVertexClass("City");
    type.createProperty("name", OType.STRING);

    db.command(new OCommandSQL("create index City.name on City (name) FULLTEXT ENGINE LUCENE")).execute();
  }

  @Test
  public void dropIndex() {
    db.command(new OCommandSQL("drop index City.name")).execute();
  }

}
