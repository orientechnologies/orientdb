/*
 *
 *  * Copyright 2014 Orient Technologies.
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

package com.orientechnologies.test;

import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.testng.annotations.Test;

import java.io.InputStream;

/**
 * Created by enricorisa on 26/09/14.
 */
@Test(groups = "remote")
public class LuceneCreateIndexRemote extends LuceneCreateIndexTest {

  public LuceneCreateIndexRemote() {
    super(true);
  }

  @Override
  public void loadAndTest() {
    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");

    databaseDocumentTx.command(new OCommandScript("sql", getScriptFromStream(stream))).execute();

    databaseDocumentTx.command(new OCommandSQL("create index Song.title on Song (title) FULLTEXT ENGINE LUCENE")).execute();
    databaseDocumentTx.command(new OCommandSQL("create index Song.author on Song (author) FULLTEXT ENGINE LUCENE")).execute();

    assertQuery();

    restart();

    assertQuery();
  }
}
