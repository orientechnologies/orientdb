/*
 * Copyright 2014 Orient Technologies.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.lucene;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.lucene.manager.OLuceneIndexManagerAbstract;
import com.orientechnologies.lucene.operator.OLuceneNearOperator;
import com.orientechnologies.lucene.operator.OLuceneTextOperator;
import com.orientechnologies.lucene.operator.OLuceneWithinOperator;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexes;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;

public class OLuceneIndexPlugin extends OServerPluginAbstract implements ODatabaseLifecycleListener {

  public OLuceneIndexPlugin() {
  }

  @Override
  public String getName() {
    return "lucene-index";
  }

  @Override
  public void startup() {
    super.startup();
//    Orient.instance().addDbLifecycleListener(this);
//
//    OIndexes.registerFactory(new OLuceneIndexFactory(true));
//    OSQLEngine.registerOperator(new OLuceneTextOperator());
//    OSQLEngine.registerOperator(new OLuceneWithinOperator());
//    OSQLEngine.registerOperator(new OLuceneNearOperator());
//    OLogManager.instance().info(this, "Lucene index plugin installed and active. Lucene version: %s",
//        OLuceneIndexManagerAbstract.LUCENE_VERSION);
  }

  @Override
  public void config(OServer oServer, OServerParameterConfiguration[] iParams) {

  }

  @Override
  public void shutdown() {
    super.shutdown();
  }

  @Override
  public PRIORITY getPriority() {
    return PRIORITY.REGULAR;
  }

  @Override
  public void onCreate(final ODatabaseInternal iDatabase) {
  }

  @Override
  public void onOpen(final ODatabaseInternal iDatabase) {
  }

  @Override
  public void onClose(final ODatabaseInternal iDatabase) {
  }

  @Override
  public void onDrop(final ODatabaseInternal iDatabase) {
    OLogManager.instance().info(this, "Dropping Lucene indexes...");
    for (OIndex idx : iDatabase.getMetadata().getIndexManager().getIndexes()) {
      if (idx.getInternal() instanceof OLuceneIndex) {
        OLogManager.instance().info(this, "- index '%s'", idx.getName());
        idx.delete();
      }
    }
  }

  @Override
  public void onCreateClass(final ODatabaseInternal iDatabase, final OClass iClass) {
  }

  @Override
  public void onDropClass(final ODatabaseInternal iDatabase, final OClass iClass) {
  }
}
