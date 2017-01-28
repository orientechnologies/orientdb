/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (info(-at-)orientdb.com)
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

package com.orientechnologies.orient.etl.loader;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.etl.OETLComponent;
import com.orientechnologies.orient.etl.OETLPipeline;

/**
 * ETL Loader.
 */
public interface OETLLoader extends OETLComponent {

  void load(ODatabaseDocument db, final Object input, OCommandContext context);

  void beginLoader(OETLPipeline pipeline);

  long getProgress();

  String getUnit();

  void rollback(ODatabaseDocument db);

  ODatabasePool getPool();

  void close();
}
