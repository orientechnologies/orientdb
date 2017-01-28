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
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * ETL Loader that saves record into OrientDB database.
 */
public class OETLOutputLoader extends OETLAbstractLoader {
  @Override
  public void load(ODatabaseDocument db, final Object input, final OCommandContext context) {
    progress.incrementAndGet();
    System.out.println(input);
  }

  @Override
  public String getUnit() {
    return "bytes";
  }

  @Override
  public void rollback(ODatabaseDocument db) {
  }

  @Override
  public ODatabasePool getPool() {
    return null;
  }

  @Override
  public ODocument getConfiguration() {
    return new ODocument();
  }

  @Override
  public String getName() {
    return "output";
  }

  @Override
  public void close() {

  }
}
