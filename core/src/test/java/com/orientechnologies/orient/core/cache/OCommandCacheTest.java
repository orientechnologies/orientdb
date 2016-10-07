/*
 *
 *  *  Copyright 2015 OrientDB LTD (info(-at-)orientdb.com)
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
package com.orientechnologies.orient.core.cache;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Created by Enrico Risa on 25/11/15.
 */
public class OCommandCacheTest {

  @Test
  public void testCommandCache() {

    OGlobalConfiguration.COMMAND_CACHE_ENABLED.setValue(true);
    OGlobalConfiguration.COMMAND_CACHE_MIN_EXECUTION_TIME.setValue(1);
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + OCommandCacheTest.class.getSimpleName());
    db.create();

    try {
      db.getMetadata().getSchema().createClass("OCommandCache");

      for (int i = 0; i < 200; i++) {
        ODocument doc = new ODocument("OCommandCache");
        db.save(doc);
      }
      OSQLSynchQuery<List<ODocument>> query = new OSQLSynchQuery<List<ODocument>>("select from OCommandCache");
      query.setCacheableResult(true);
      List<ODocument> results = db.query(query);

      OCommandCache commandCache = db.getMetadata().getCommandCache();
      Collection cachedResults = (Collection) commandCache.get(new OUser("admin"), "select from OCommandCache", -1);

      Assert.assertNotNull(cachedResults);
      Assert.assertEquals(results.size(), cachedResults.size());
    } finally {
      db.drop();
    }
  }

  @Test
  public void testCommandCacheConfiguration() {

    OGlobalConfiguration.COMMAND_CACHE_ENABLED.setValue(true);
    OGlobalConfiguration.COMMAND_CACHE_MIN_EXECUTION_TIME.setValue(1);
    OGlobalConfiguration.COMMAND_CACHE_MAX_RESULSET_SIZE.setValue(10);
    OGlobalConfiguration.COMMAND_CACHE_EVICT_STRATEGY.setValue(OCommandCache.STRATEGY.INVALIDATE_ALL);

    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null)
      buildDirectory = ".";

    String dbPath = "plocal:" + buildDirectory + File.separator + OCommandCacheTest.class.getSimpleName();
    ODatabaseDocument db = new ODatabaseDocumentTx(dbPath);

    db.create();

    try {

      File commandCacheCfg = new File(
          buildDirectory + File.separator + OCommandCacheTest.class.getSimpleName() + "/command-cache.json");
      final String configurationContent = OIOUtils.readFileAsString(commandCacheCfg);
      ODocument cfg = new ODocument().fromJSON(configurationContent);

      Boolean enabled = cfg.field("enabled");
      String evict = cfg.field("evictStrategy");
      OCommandCache.STRATEGY evictStrategy = OCommandCache.STRATEGY.valueOf(evict);
      int minExecutionTime = cfg.field("minExecutionTime");
      int maxResultsetSize = cfg.field("maxResultsetSize");
      Assert.assertEquals(enabled, OGlobalConfiguration.COMMAND_CACHE_ENABLED.getValue());
      Assert.assertEquals(evictStrategy.toString(), OGlobalConfiguration.COMMAND_CACHE_EVICT_STRATEGY.getValue().toString());
      Assert.assertEquals(minExecutionTime, OGlobalConfiguration.COMMAND_CACHE_MIN_EXECUTION_TIME.getValue());
      Assert.assertEquals(maxResultsetSize, OGlobalConfiguration.COMMAND_CACHE_MAX_RESULSET_SIZE.getValue());

    } catch (IOException e) {
      Assert.fail("Cannot find file configuration");

    } finally {
      db.drop();
    }

    File f = new File(buildDirectory + File.separator + OCommandCacheTest.class.getSimpleName());

    Assert.assertEquals(f.exists(), false);
  }
}
