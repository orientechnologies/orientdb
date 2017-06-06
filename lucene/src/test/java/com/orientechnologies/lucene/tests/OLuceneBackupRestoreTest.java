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

import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by Enrico Risa on 07/07/15.
 */
@RunWith(JUnit4.class)
public class OLuceneBackupRestoreTest extends OLuceneBaseTest {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    final String os = System.getProperty("os.name").toLowerCase(Locale.ENGLISH);
    Assume.assumeFalse(os.contains("win"));


    dropDatabase();
    super.setupDatabase("ci");
    db.command("create class City ");
    db.command("create property City.name string");
    db.command("create index City.name on City (name) FULLTEXT ENGINE LUCENE");

    ODocument doc = new ODocument("City");
    doc.field("name", "Rome");
    db.save(doc);
  }

  @Test
  public void shouldBackupAndRestore() throws IOException {

    File backupFile = tempFolder.newFile("backupRestore.gz");

    OResultSet query = db.query("select from City where search_class('Rome') = true");

    assertThat(query).hasSize(1);

    db.backup(new FileOutputStream(backupFile), null, null, null, 9, 1048576);

    //RESTORE
    dropDatabase();

    setupDatabase("ci");

    FileInputStream stream = new FileInputStream(backupFile);

    db.restore(stream, null, null, null);

    db.close();

    //VERIFY
    db = pool.acquire();

    assertThat(db.countClass("City")).isEqualTo(1);

    OIndex<?> index = db.getMetadata().getIndexManager().getIndex("City.name");

    assertThat(index).isNotNull();
    assertThat(index.getType()).isEqualTo(OClass.INDEX_TYPE.FULLTEXT.name());

    assertThat(db.query("select from City where search_class('Rome') = true")).hasSize(1);
  }

}
