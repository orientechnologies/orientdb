/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.cache.local.OWOWCache;
import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMap;
import com.orientechnologies.orient.core.storage.cluster.OPaginatedCluster;
import com.orientechnologies.orient.core.storage.disk.OLocalPaginatedStorage;
import java.io.File;
import java.util.Collection;
import java.util.Locale;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "sql-delete", sequential = true)
public class SQLCommandsTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public SQLCommandsTest(@Optional String url) {
    super(url);
  }

  public void createProperty() {
    OSchema schema = database.getMetadata().getSchema();
    if (!schema.existsClass("account")) schema.createClass("account");

    database.command(new OCommandSQL("create property account.timesheet string")).execute();

    Assert.assertEquals(
        database.getMetadata().getSchema().getClass("account").getProperty("timesheet").getType(),
        OType.STRING);
  }

  @Test(dependsOnMethods = "createProperty")
  public void createLinkedClassProperty() {
    database
        .command(new OCommandSQL("create property account.knows embeddedmap account"))
        .execute();

    Assert.assertEquals(
        database.getMetadata().getSchema().getClass("account").getProperty("knows").getType(),
        OType.EMBEDDEDMAP);
    Assert.assertEquals(
        database
            .getMetadata()
            .getSchema()
            .getClass("account")
            .getProperty("knows")
            .getLinkedClass(),
        database.getMetadata().getSchema().getClass("account"));
  }

  @Test(dependsOnMethods = "createLinkedClassProperty")
  public void createLinkedTypeProperty() {
    database.command(new OCommandSQL("create property account.tags embeddedlist string")).execute();

    Assert.assertEquals(
        database.getMetadata().getSchema().getClass("account").getProperty("tags").getType(),
        OType.EMBEDDEDLIST);
    Assert.assertEquals(
        database.getMetadata().getSchema().getClass("account").getProperty("tags").getLinkedType(),
        OType.STRING);
  }

  @Test(dependsOnMethods = "createLinkedTypeProperty")
  public void removeProperty() {
    database.command(new OCommandSQL("drop property account.timesheet")).execute();
    database.command(new OCommandSQL("drop property account.tags")).execute();

    Assert.assertFalse(
        database.getMetadata().getSchema().getClass("account").existsProperty("timesheet"));
    Assert.assertFalse(
        database.getMetadata().getSchema().getClass("account").existsProperty("tags"));
  }

  @Test(dependsOnMethods = "removeProperty")
  public void testSQLScript() {
    String cmd = "";
    cmd += "select from ouser limit 1;begin;";
    cmd += "let a = create vertex set script = true\n";
    cmd += "let b = select from v limit 1;";
    cmd += "create edge from $a to $b;";
    cmd += "commit;";
    cmd += "return $a;";

    Object result = database.command(new OCommandScript("sql", cmd)).execute();

    Assert.assertTrue(result instanceof OIdentifiable);
    Assert.assertTrue(((OIdentifiable) result).getRecord() instanceof ODocument);
    Assert.assertTrue((Boolean) ((ODocument) ((OIdentifiable) result).getRecord()).field("script"));
  }

  public void testClusterRename() {
    if (database.getURL().startsWith("memory:")) return;

    Collection<String> names = database.getClusterNames();
    Assert.assertFalse(names.contains("testClusterRename".toLowerCase(Locale.ENGLISH)));

    database.command(new OCommandSQL("create cluster testClusterRename")).execute();

    names = database.getClusterNames();
    Assert.assertTrue(names.contains("testClusterRename".toLowerCase(Locale.ENGLISH)));

    database
        .command(new OCommandSQL("alter cluster testClusterRename name testClusterRename42"))
        .execute();
    names = database.getClusterNames();

    Assert.assertTrue(names.contains("testClusterRename42".toLowerCase(Locale.ENGLISH)));
    Assert.assertFalse(names.contains("testClusterRename".toLowerCase(Locale.ENGLISH)));

    if (database.getURL().startsWith("plocal:")) {
      String storagePath = database.getStorage().getConfiguration().getDirectory();

      final OWOWCache wowCache =
          (OWOWCache) ((OLocalPaginatedStorage) database.getStorage()).getWriteCache();

      File dataFile =
          new File(
              storagePath,
              wowCache.nativeFileNameById(
                  wowCache.fileIdByName("testClusterRename42" + OPaginatedCluster.DEF_EXTENSION)));
      File mapFile =
          new File(
              storagePath,
              wowCache.nativeFileNameById(
                  wowCache.fileIdByName(
                      "testClusterRename42" + OClusterPositionMap.DEF_EXTENSION)));

      Assert.assertTrue(dataFile.exists());
      Assert.assertTrue(mapFile.exists());
    }
  }
}
