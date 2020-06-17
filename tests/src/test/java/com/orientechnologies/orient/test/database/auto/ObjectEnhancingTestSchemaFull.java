/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.client.db.ODatabaseHelper;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.object.db.OObjectDatabasePool;
import com.orientechnologies.orient.object.enhancement.OObjectEntityEnhancer;
import com.orientechnologies.orient.object.enhancement.OObjectMethodFilter;
import com.orientechnologies.orient.test.domain.base.CustomMethodFilterTestClass;
import java.io.IOException;
import java.lang.reflect.Method;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(
    groups = {"object", "enhancingSchemaFull"},
    dependsOnGroups = "detachingSchemaFull")
public class ObjectEnhancingTestSchemaFull extends ObjectDBBaseTest {

  @Parameters(value = "url")
  public ObjectEnhancingTestSchemaFull(@Optional String url) {
    super(url, "_objectschema");
  }

  @Test()
  public void testCustomMethodFilter() {
    OObjectEntityEnhancer.getInstance()
        .registerClassMethodFilter(CustomMethodFilterTestClass.class, new CustomMethodFilter());
    CustomMethodFilterTestClass testClass = database.newInstance(CustomMethodFilterTestClass.class);
    testClass.setStandardField("testStandard");
    testClass.setUPPERCASEFIELD("testUpperCase");
    testClass.setTransientNotDefinedField("testTransient");
    Assert.assertNull(testClass.getStandardFieldAsList());
    Assert.assertNull(testClass.getStandardFieldAsMap());
    database.save(testClass);
    ORID rid = database.getIdentity(testClass);
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    testClass = database.load(rid);
    Assert.assertEquals(testClass.getStandardField(), "testStandard");
    Assert.assertEquals(testClass.getUPPERCASEFIELD(), "testUpperCase");
    Assert.assertNull(testClass.getStandardFieldAsList());
    Assert.assertNull(testClass.getStandardFieldAsMap());
    ODocument doc = database.getRecordByUserObject(testClass, false);
    Assert.assertTrue(!doc.containsField("transientNotDefinedField"));
  }

  public class CustomMethodFilter extends OObjectMethodFilter {
    @Override
    public boolean isHandled(Method m) {
      if (m.getName().contains("UPPERCASE")) {
        return true;
      } else if (m.getName().contains("Transient")) {
        return false;
      }
      return super.isHandled(m);
    }

    @Override
    public String getFieldName(Method m) {
      if (m.getName().startsWith("get")) {
        if (m.getName().contains("UPPERCASE")) {
          return "UPPERCASEFIELD";
        }
        return getFieldName(m.getName(), "get");
      } else if (m.getName().startsWith("set")) {
        if (m.getName().contains("UPPERCASE")) {
          return "UPPERCASEFIELD";
        }
        return getFieldName(m.getName(), "set");
      } else return getFieldName(m.getName(), "is");
    }

    @Override
    protected String getFieldName(String methodName, String prefix) {
      StringBuffer fieldName = new StringBuffer();
      fieldName.append(Character.toLowerCase(methodName.charAt(prefix.length())));
      for (int i = (prefix.length() + 1); i < methodName.length(); i++) {
        fieldName.append(methodName.charAt(i));
      }
      return fieldName.toString();
    }
  }

  @AfterClass
  public void end() throws IOException {
    String prefix = url.substring(0, url.indexOf(':') + 1);
    OLogManager.instance().info(this, "deleting database " + url);
    ODatabaseHelper.dropDatabase(
        OObjectDatabasePool.global().acquire(url, "admin", "admin"), prefix);
  }
}
