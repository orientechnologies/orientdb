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
package com.orientechnologies.orient.server.config;

import org.junit.Assert;
import org.junit.Test;

public class OServerConfigurationManagerTest {
  @Test
  public void testManagerUsers() {
    final OServerConfigurationManager cfgManager =
        new OServerConfigurationManager(new OServerConfiguration());

    Assert.assertNull(cfgManager.getConfiguration().users);

    // ADD USERS AND REMOVE FROM THE END
    cfgManager.setUser("a0", "b", "c");

    Assert.assertNotNull(cfgManager.getConfiguration().users);
    Assert.assertEquals(cfgManager.getConfiguration().users.length, 1);

    cfgManager.setUser("a1", "b", "c");
    cfgManager.setUser("a2", "b", "c");
    cfgManager.setUser("a3", "b", "c");

    Assert.assertEquals(cfgManager.getConfiguration().users.length, 4);

    Assert.assertEquals(cfgManager.getUsers().size(), 4);

    Assert.assertTrue(cfgManager.existsUser("a0"));
    Assert.assertTrue(cfgManager.existsUser("A1"));
    Assert.assertTrue(cfgManager.existsUser("a2"));
    Assert.assertTrue(cfgManager.existsUser("A0"));

    Assert.assertFalse(cfgManager.existsUser("A00"));

    cfgManager.dropUser("A3");
    cfgManager.dropUser("A2");
    cfgManager.dropUser("A1");
    cfgManager.dropUser("A0");

    Assert.assertEquals(cfgManager.getConfiguration().users.length, 0);

    // ADD USERS AND REMOVE FROM THE BEGINNING
    cfgManager.setUser("a0", "b", "c");

    Assert.assertEquals(cfgManager.getConfiguration().users.length, 1);

    cfgManager.setUser("a1", "b", "c");
    cfgManager.setUser("a2", "b", "c");
    cfgManager.setUser("a3", "b", "c");

    Assert.assertEquals(cfgManager.getConfiguration().users.length, 4);

    cfgManager.dropUser("A0");
    cfgManager.dropUser("A1");
    cfgManager.dropUser("A2");
    cfgManager.dropUser("A3");

    Assert.assertEquals(cfgManager.getConfiguration().users.length, 0);

    // ADD USERS AND REMOVE FROM THE MIDDLE
    cfgManager.setUser("a0", "b", "c");

    Assert.assertEquals(cfgManager.getConfiguration().users.length, 1);

    cfgManager.setUser("a1", "b", "c");
    cfgManager.setUser("a2", "b", "c");
    cfgManager.setUser("a3", "b", "c");

    Assert.assertEquals(cfgManager.getConfiguration().users.length, 4);

    cfgManager.dropUser("A2");
    cfgManager.dropUser("A1");
    cfgManager.dropUser("A0");
    cfgManager.dropUser("A3");

    Assert.assertEquals(cfgManager.getConfiguration().users.length, 0);
  }
}
