/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.database.auto;

import java.io.File;
import java.io.IOException;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;

@Test(groups = "db")
public class DbImportTest implements OCommandOutputListener {

  @Parameters(value = "url")
  public DbImportTest(String iURL) {
    OProfiler.getInstance().startRecording();
  }

  public void testDbImport() throws IOException {
    File importDir = new File("tests/target/test-import");

    if (importDir.exists())
      for (File f : importDir.listFiles())
        f.delete();
    else
      importDir.mkdir();

    ODatabaseDocumentTx database = new ODatabaseDocumentTx("local:tests/target/test-import/test-import");
    database.create();

    ODatabaseImport impor = new ODatabaseImport(database, "tests/target/db.export", this);
    impor.importDatabase();
    impor.close();

    database.close();

    for (File f : importDir.listFiles())
      f.delete();
    importDir.delete();
  }

  @Test(enabled = false)
  public void onMessage(final String iText) {
    System.out.print(iText);
    System.out.flush();
  }
}
