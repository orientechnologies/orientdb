/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

package com.orientechnologies.orient.etl;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.loader.OETLAbstractLoader;
import java.util.ArrayList;
import java.util.List;

/**
 * ETL Stub loader to check the result in tests.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com) on 27/11/14.
 */
public class OETLStubLoader extends OETLAbstractLoader {
  public final List<ODocument> loadedRecords = new ArrayList<ODocument>();
  private ODatabasePool pool;
  private OrientDB orient;

  public OETLStubLoader() {}

  @Override
  public void beginLoader(OETLPipeline pipeline) {

    orient = new OrientDB("embedded:", null);

    orient.execute(
        "create database testDatabase memory users (admin identified by 'admin' role admin)");

    pool = new ODatabasePool(orient, "testDatabase", "admin", "admin");
    pipeline.setPool(pool);
  }

  @Override
  public void load(ODatabaseDocument db, Object input, OCommandContext context) {
    synchronized (loadedRecords) {
      loadedRecords.add((ODocument) input);
      progress.incrementAndGet();
    }
  }

  @Override
  public String getUnit() {
    return "document";
  }

  @Override
  public void rollback(ODatabaseDocument db) {}

  @Override
  public ODatabasePool getPool() {
    return pool;
  }

  @Override
  public void close() {
    orient.close();
  }

  @Override
  public String getName() {
    return "test";
  }
}
