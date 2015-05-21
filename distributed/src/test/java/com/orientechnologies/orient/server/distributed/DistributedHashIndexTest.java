package com.orientechnologies.orient.server.distributed;

import junit.framework.Assert;
import org.junit.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.sql.OCommandSQL;

/*
 *
 *  *  Copyright 2015 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */

/**
 * @author Luigi Dell'Aquila
 */
public class DistributedHashIndexTest extends AbstractServerClusterTest {
  private final static int SERVERS = 2;

  public String getDatabaseName() {
    return "DistributedHashIndexTest";
  }

  @Test
  public void test() throws Exception {
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  protected void executeTest() throws Exception {

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:target/server1/databases/" + getDatabaseName());

    db.open("admin", "admin");

    db.command(new OCommandSQL("create class DistributedHashIndexTest"));
    db.command(new OCommandSQL("create property DistributedHashIndexTest.name STRING"));
    try {
      db.command(new OCommandSQL(
          "CREATE INDEX index_DistributedHashIndexTest_name ON DistributedHashIndexTest (name) UNIQUE_HASH_INDEX"));
    }catch(Exception e){
      Assert.fail();
    }
  }
}