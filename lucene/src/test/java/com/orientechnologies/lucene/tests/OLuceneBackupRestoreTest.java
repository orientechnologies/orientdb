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

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by Enrico Risa on 07/07/15.
 */
public class OLuceneBackupRestoreTest {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private ODatabaseDocumentTx databaseDocumentTx;

  @Before
  public void setUp() throws Exception {

    String url = "plocal:./target/" + getClass().getName();

    databaseDocumentTx = new ODatabaseDocumentTx(url);

    dropIfExists();

    databaseDocumentTx.create();

    databaseDocumentTx.command(new OCommandSQL("create class City ")).execute();
    databaseDocumentTx.command(new OCommandSQL("create property City.name string")).execute();
    databaseDocumentTx.command(new OCommandSQL("create index City.name on City (name) FULLTEXT ENGINE LUCENE")).execute();

    ODocument doc = new ODocument("City");
    doc.field("name", "Rome");
    databaseDocumentTx.save(doc);
  }

  private void dropIfExists() {
    if (databaseDocumentTx.exists()) {
      if (databaseDocumentTx.isClosed())
        databaseDocumentTx.open("admin", "admin");
      databaseDocumentTx.drop();
    }
  }

  @After
  public void tearDown() throws Exception {
    dropIfExists();

  }

  @Test
  public void shouldBackupAndRestore() throws IOException {

    File backupFile = tempFolder.newFile("backupRestore.gz");

    OResultSet query = databaseDocumentTx.query("select from City where search_class('Rome') = true");

    assertThat(query).hasSize(1);

    databaseDocumentTx.backup(new FileOutputStream(backupFile), null, null, null, 9, 1048576);

    //RESTORE
    databaseDocumentTx.drop();

    databaseDocumentTx.create();

    FileInputStream stream = new FileInputStream(backupFile);

    databaseDocumentTx.restore(stream, null, null, null);

    databaseDocumentTx.close();

    //VERIFY
    databaseDocumentTx.open("admin", "admin");

    assertThat(databaseDocumentTx.countClass("City")).isEqualTo(1);

    OIndex<?> index = databaseDocumentTx.getMetadata().getIndexManager().getIndex("City.name");

    assertThat(index).isNotNull();
    assertThat(index.getType()).isEqualTo(OClass.INDEX_TYPE.FULLTEXT.name());

    assertThat(databaseDocumentTx.query("select from City where search_class('Rome') = true")).hasSize(1);
  }

}
