/*
 *
 *  *  Copyright 2016 OrientDB LTD (info(at)orientdb.com)
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
 *  * For more information: http://www.orientdb.com
 */

package com.orientechnologies.orient.core.storage;

import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.exception.OInvalidDatabaseNameException;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Sergey Sitnikov
 */
public class StorageNamingTests {

  @Test
  public void testSpecialLettersOne() {
    try (OrientDB orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig())) {
      try {
        orientDB.create("name%", ODatabaseType.MEMORY);
        Assert.fail();
      } catch (OInvalidDatabaseNameException e) {
        // skip
      }
    }
  }

  @Test
  public void testSpecialLettersTwo() {
    try (OrientDB orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig())) {
      try {
        orientDB.create("na.me", ODatabaseType.MEMORY);
        Assert.fail();
      } catch (OInvalidDatabaseNameException e) {
        // skip
      }
    }
  }

  @Test
  public void testSpecialLettersThree() {
    try (OrientDB orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig())) {
      orientDB.create("na_me$", ODatabaseType.MEMORY);
      orientDB.drop("na_me$");
    }
  }

  @Test
  public void commaInPathShouldBeAllowed() {
    OAbstractPaginatedStorage.checkName("/path/with/,/but/not/in/the/name");
    OAbstractPaginatedStorage.checkName("/,,,/,/,/name");
  }

  @Test(expected = IllegalArgumentException.class)
  public void commaInNameShouldThrow() {

    OAbstractPaginatedStorage.checkName("/path/with/,/name/with,");

    //    Assert.assertThrows(IllegalArgumentException.class, new Assert.ThrowingRunnable() {
    //      @Override
    //      public void run() throws Throwable {
    //        new NamingTestStorage("/name/with,");
    //      }
    //    });
  }

  @Test(expected = IllegalArgumentException.class)
  public void name() throws Exception {
    OAbstractPaginatedStorage.checkName("/name/with,");
  }
}
