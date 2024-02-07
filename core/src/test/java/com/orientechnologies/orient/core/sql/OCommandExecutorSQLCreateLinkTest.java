/*
 *
 *  *  Copyright 2015 OrientDB LTD (info(at)orientdb.com)
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
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class OCommandExecutorSQLCreateLinkTest extends BaseMemoryDatabase {

  @Test
  public void testBasic() throws Exception {
    db.command("create class Basic1").close();
    db.command("create class Basic2").close();

    db.command("insert into Basic1 set pk = 'pkb1_1', fk = 'pkb2_1'").close();
    db.command("insert into Basic1 set pk = 'pkb1_2', fk = 'pkb2_2'").close();

    db.command("insert into Basic2 set pk = 'pkb2_1'").close();
    db.command("insert into Basic2 set pk = 'pkb2_2'").close();

    db.command("CREATE LINK theLink type link FROM Basic1.fk TO Basic2.pk ").close();

    OResultSet result = db.query("select pk, theLink.pk as other from Basic1 order by pk");

    Object otherKey = result.next().getProperty("other");
    Assert.assertNotNull(otherKey);

    Assert.assertEquals(otherKey, "pkb2_1");

    otherKey = result.next().getProperty("other");
    Assert.assertEquals(otherKey, "pkb2_2");
  }

  @Test
  public void testInverse() throws Exception {
    db.command("create class Inverse1").close();
    db.command("create class Inverse2").close();

    db.command("insert into Inverse1 set pk = 'pkb1_1', fk = 'pkb2_1'").close();
    db.command("insert into Inverse1 set pk = 'pkb1_2', fk = 'pkb2_2'").close();
    db.command("insert into Inverse1 set pk = 'pkb1_3', fk = 'pkb2_2'").close();

    db.command("insert into Inverse2 set pk = 'pkb2_1'").close();
    db.command("insert into Inverse2 set pk = 'pkb2_2'").close();

    db.command("CREATE LINK theLink TYPE LINKSET FROM Inverse1.fk TO Inverse2.pk INVERSE").close();

    OResultSet result = db.query("select pk, theLink.pk as other from Inverse2 order by pk");
    OResult first = result.next();
    Object otherKeys = first.getProperty("other");
    Assert.assertNotNull(otherKeys);
    Assert.assertTrue(otherKeys instanceof List);
    Assert.assertEquals(((List) otherKeys).get(0), "pkb1_1");

    OResult second = result.next();
    otherKeys = second.getProperty("other");
    Assert.assertNotNull(otherKeys);
    Assert.assertTrue(otherKeys instanceof List);
    Assert.assertEquals(((List) otherKeys).size(), 2);
    Assert.assertTrue(((List) otherKeys).contains("pkb1_2"));
    Assert.assertTrue(((List) otherKeys).contains("pkb1_3"));
  }
}
