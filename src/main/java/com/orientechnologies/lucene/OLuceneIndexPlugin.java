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

import com.orientechnologies.common.util.OClassLoaderHelper;
import com.orientechnologies.lucene.operator.OLuceneNearOperator;
import com.orientechnologies.lucene.operator.OLuceneTextOperator;
import com.orientechnologies.lucene.operator.OLuceneWithinOperator;
import com.orientechnologies.orient.core.index.OIndexes;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;

import javax.imageio.spi.ServiceRegistry;
import java.util.Iterator;

public class OLuceneIndexPlugin extends OServerPluginAbstract {

  public OLuceneIndexPlugin() {
  }

  @Override
  public String getName() {
    return "lucene-index";
  }

  @Override
  public void startup() {
    super.startup();
    // Orient.instance().addDbLifecycleListener(new OLuceneClassIndexManager());

    OIndexes.registerFactory(new OLuceneIndexFactory());
    OSQLEngine.registerOperator(new OLuceneTextOperator());
    OSQLEngine.registerOperator(new OLuceneWithinOperator());
    OSQLEngine.registerOperator(new OLuceneNearOperator());
  }

  @Override
  public void config(OServer oServer, OServerParameterConfiguration[] iParams) {

  }
}
