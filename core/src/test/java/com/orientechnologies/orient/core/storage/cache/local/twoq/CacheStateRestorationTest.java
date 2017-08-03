/*
 *
 *  *  Copyright 2010-2017 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.storage.cache.local.twoq;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * @author Sergey Sitnikov
 */
public class CacheStateRestorationTest {

  private String              buildDirectory;
  private String              dbDirectory;
  private String              cacheStateCopyFile;
  private ODatabaseDocumentTx db;

  @Before
  public void before() {
    buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null)
      buildDirectory = ".";
    dbDirectory = buildDirectory + '/' + CacheStateRestorationTest.class.getSimpleName();
    cacheStateCopyFile = buildDirectory + "/" + CacheStateRestorationTest.class.getSimpleName() + "-" + O2QCache.CACHE_STATE_FILE;

    db = new ODatabaseDocumentTx("plocal:" + dbDirectory);
    if (db.exists()) {
      db.open("admin", "admin");
      db.drop();
    }
    db.create();
  }

  @After
  public void after() throws IOException {
    db.drop();
    OFileUtils.delete(new File(cacheStateCopyFile));
  }

  @Test
  public void testNoFailuresOnOutdatedCacheStateFile() throws IOException {

    // 1. Create the test cluster to populate the cache.

    db.getMetadata().getSchema().createClass("Doc");
    final ODocument doc = db.newInstance("Doc");
    doc.field("field", "value").save();

    // 2. Close the database and shutdown OrientDB runtime to force the cache state file creation.

    db.close();
    Orient.instance().shutdown();

    // 3. Backup the cache state file.

    OFileUtils.copyFile(new File(dbDirectory + "/" + O2QCache.CACHE_STATE_FILE), new File(cacheStateCopyFile));

    // 4. Restart OrientDB runtime and delete the test cluster.

    Orient.instance().startup();
    db = new ODatabaseDocumentTx("plocal:" + dbDirectory);
    db.open("admin", "admin");
    db.getMetadata().getSchema().dropClass("Doc");

    // 5. Shutdown the runtime again.

    db.close();
    Orient.instance().shutdown();

    // 6. Replace the cache state file by the old one containing invalid information about deleted files.

    OFileUtils.copyFile(new File(cacheStateCopyFile), new File(dbDirectory + "/" + O2QCache.CACHE_STATE_FILE));

    // 7. Start the runtime again and open the database to force the loading of the invalid cache state file.

    Orient.instance().startup();
    db = new ODatabaseDocumentTx("plocal:" + dbDirectory);
    db.open("admin", "admin"); // should not throw
  }

}
