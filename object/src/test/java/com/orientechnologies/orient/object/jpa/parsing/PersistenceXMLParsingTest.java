/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIESOR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.orientechnologies.orient.object.jpa.parsing;

import static java.util.Arrays.asList;
import static java.util.Collections.EMPTY_LIST;
import static javax.persistence.spi.PersistenceUnitTransactionType.JTA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import com.orientechnologies.orient.object.jpa.OJPAPersistenceUnitInfo;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import javax.persistence.PersistenceException;
import javax.persistence.SharedCacheMode;
import javax.persistence.ValidationMode;
import javax.persistence.spi.PersistenceUnitInfo;
import javax.persistence.spi.PersistenceUnitTransactionType;
import org.junit.Assert;
import org.junit.Test;

public class PersistenceXMLParsingTest {

  public static void assertPersistenceUnit(PersistenceUnitInfo unit, Rule[] rules)
      throws IllegalAccessException, IllegalArgumentException, InvocationTargetException,
          NoSuchMethodException, SecurityException {
    for (int i = 0; i < rules.length; i++) {
      Method method = OJPAPersistenceUnitInfo.class.getDeclaredMethod(rules[i].method);
      String msg =
          "For unit name '"
              + (unit.getPersistenceUnitName() != null ? unit.getPersistenceUnitName() : "-empty-")
              + "': "
              + rules[i].message;
      assertEquals(msg, rules[i].expected, method.invoke(unit));
    }
  }

  /**
   * @param a array of strings
   * @return array of urls using @see
   *     com.orientechnologies.orient.object.jpa.parsing.OJPAPersistenceUnitInfo.initJarFile(String)
   */
  public static List<URL> asURLList(String... a) {
    List<URL> list = new ArrayList<URL>();
    for (int i = 0; i < a.length; i++) {
      list.add(OJPAPersistenceUnitInfo.initJarFile(a[i]));
    }
    return list;
  }

  /**
   * @param a put 'key1, value1, key2, value2, ...' here
   * @return
   */
  public static Properties asProperty(String... a) {
    Properties properties = new Properties();
    for (int i = 0; i < a.length; i += 2) {
      properties.setProperty(a[i], a[i + 1]);
    }
    return properties;
  }

  /**
   * Test parsing a persistence descriptor with several entries
   *
   * @throws Exception
   */
  @Test
  public void testFile1() throws Exception {
    URL locationUrl =
        getClass().getClassLoader().getResource("apache-aries/file1/META-INF/persistence.xml");
    Collection<? extends PersistenceUnitInfo> parsedUnits = PersistenceXmlUtil.parse(locationUrl);

    Assert.assertNotNull("Persistence units shouldn't be null.", parsedUnits);
    assertEquals(
        "An incorrect number of persistence units has been returned.", 4, parsedUnits.size());
    Iterator<? extends PersistenceUnitInfo> iterator = parsedUnits.iterator();

    assertPersistenceUnit(
        iterator.next(),
        new Rule[] { //
          new Rule("The schema version was incorrect", "1.0", "getPersistenceXMLSchemaVersion"),
          new Rule("The unit name was incorrect", "alpha", "getPersistenceUnitName"),
          new Rule("The transaction type was incorrect", null, "getTransactionType"),
          new Rule(
              "The provider class name was incorrect", null, "getPersistenceProviderClassName"),
          new Rule("One or more mapping files were specified", EMPTY_LIST, "getMappingFileNames"),
          new Rule("One or more jar files were specified", EMPTY_LIST, "getJarFileUrls"),
          new Rule(
              "One or more managed classes were specified", EMPTY_LIST, "getManagedClassNames"),
          new Rule("We should not exclude any classes", false, "excludeUnlistedClasses")
        });

    assertPersistenceUnit(
        iterator.next(),
        new Rule[] { //
          new Rule("The schema version was incorrect", "1.0", "getPersistenceXMLSchemaVersion"),
          new Rule("The unit name was incorrect", "bravo", "getPersistenceUnitName"),
          new Rule("The transaction type was incorrect", JTA, "getTransactionType"),
          new Rule(
              "The provider class name was incorrect",
              "bravo.persistence.provider",
              "getPersistenceProviderClassName"),
          new Rule(
              "Incorrect mapping files were listed",
              asList("bravoMappingFile1.xml", "bravoMappingFile2.xml"),
              "getMappingFileNames"),
          new Rule(
              "Incorrect jar URLs were listed",
              asURLList("bravoJarFile1.jar", "bravoJarFile2.jar"),
              "getJarFileUrls"),
          new Rule(
              "Incorrect managed classes were listed",
              asList("bravoClass1", "bravoClass2"),
              "getManagedClassNames"),
          new Rule("We should not exclude any classes", true, "excludeUnlistedClasses"),
          new Rule(
              "The properties should never be null",
              asProperty("some.prop", "prop.value", "some.other.prop", "another.prop.value"),
              "getProperties")
        });

    assertPersistenceUnit(
        iterator.next(),
        new Rule[] { //
          new Rule("The schema version was incorrect", "1.0", "getPersistenceXMLSchemaVersion"),
          new Rule("The unit name was incorrect", "charlie", "getPersistenceUnitName"),
          new Rule(
              "The transaction type was incorrect",
              PersistenceUnitTransactionType.RESOURCE_LOCAL,
              "getTransactionType"),
          new Rule(
              "The provider class name was incorrect",
              "charlie.persistence.provider",
              "getPersistenceProviderClassName"),
          new Rule("One or more mapping files were specified", EMPTY_LIST, "getMappingFileNames"),
          new Rule("One or more jar files were specified", EMPTY_LIST, "getJarFileUrls"),
          new Rule(
              "One or more managed classes were specified", EMPTY_LIST, "getManagedClassNames"),
          new Rule("We should not exclude any classes", true, "excludeUnlistedClasses")
        });

    assertPersistenceUnit(
        iterator.next(),
        new Rule[] { //
          new Rule("The schema version was incorrect", "1.0", "getPersistenceXMLSchemaVersion"),
          new Rule("The unit name was incorrect", "delta", "getPersistenceUnitName"),
          new Rule(
              "The transaction type was incorrect",
              PersistenceUnitTransactionType.RESOURCE_LOCAL,
              "getTransactionType"),
          new Rule(
              "The provider class name was incorrect",
              "delta.persistence.provider",
              "getPersistenceProviderClassName"),
          new Rule("One or more mapping files were specified", EMPTY_LIST, "getMappingFileNames"),
          new Rule("One or more jar files were specified", EMPTY_LIST, "getJarFileUrls"),
          new Rule(
              "One or more managed classes were specified", EMPTY_LIST, "getManagedClassNames"),
          new Rule("We should not exclude any classes", false, "excludeUnlistedClasses")
        });
  }

  @Test
  public void testFile2() throws Exception {
    URL locationUrl =
        getClass().getClassLoader().getResource("apache-aries/file2/META-INF/persistence.xml");
    Collection<? extends PersistenceUnitInfo> parsedUnits = PersistenceXmlUtil.parse(locationUrl);

    assertNotNull("Persistence units shouldn't be null.", parsedUnits);
    assertEquals(
        "An incorrect number of persistence units has been returned.", 0, parsedUnits.size());
  }

  @Test(expected = PersistenceException.class)
  public void testFile3() throws Exception {
    URL locationUrl =
        getClass().getClassLoader().getResource("apache-aries/file3/META-INF/persistence.xml");
    PersistenceXmlUtil.parse(locationUrl);
    fail("Parsing should not succeed");
  }

  @Test
  public void testJPA2() throws Exception {
    URL locationUrl =
        getClass().getClassLoader().getResource("apache-aries/file22/META-INF/persistence.xml");
    Collection<? extends PersistenceUnitInfo> parsedUnits = PersistenceXmlUtil.parse(locationUrl);

    assertNotNull("Persistence units shouldn't be null.", parsedUnits);
    assertEquals(
        "An incorrect number of persistence units has been returned.", 2, parsedUnits.size());

    Iterator<? extends PersistenceUnitInfo> iterator = parsedUnits.iterator();

    // test defaults
    assertPersistenceUnit(
        iterator.next(),
        new Rule[] { //
          new Rule("The schema version was incorrect", "2.0", "getPersistenceXMLSchemaVersion"),
          new Rule("The unit name was incorrect", "default", "getPersistenceUnitName"),
          new Rule("Unexpected SharedCacheMode", SharedCacheMode.UNSPECIFIED, "getSharedCacheMode"),
          new Rule("Unexpected ValidationMode", ValidationMode.AUTO, "getValidationMode")
        });

    assertPersistenceUnit(
        iterator.next(),
        new Rule[] { //
          new Rule("The schema version was incorrect", "2.0", "getPersistenceXMLSchemaVersion"),
          new Rule("The unit name was incorrect", "custom", "getPersistenceUnitName"),
          new Rule(
              "Unexpected SharedCacheMode", SharedCacheMode.ENABLE_SELECTIVE, "getSharedCacheMode"),
          new Rule("Unexpected ValidationMode", ValidationMode.CALLBACK, "getValidationMode")
        });
  }

  /**
   * Test parsing a persistence descriptor with several entries
   *
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testReallyBigFile() throws Exception {
    URL locationUrl =
        getClass().getClassLoader().getResource("apache-aries/file24/META-INF/persistence.xml");
    Collection<? extends PersistenceUnitInfo> parsedUnits = PersistenceXmlUtil.parse(locationUrl);

    assertNotNull("Persistence units shouldn't be null.", parsedUnits);
    assertEquals(
        "An incorrect number of persistence units has been returned.", 33, parsedUnits.size());

    List<OJPAPersistenceUnitInfo> units = new ArrayList<OJPAPersistenceUnitInfo>();
    units.addAll((Collection<OJPAPersistenceUnitInfo>) parsedUnits);
    assertEquals("An incorrect number of units has been returned.", 33, units.size());

    // prepare
    Collections.sort(
        units,
        new Comparator<OJPAPersistenceUnitInfo>() {
          @Override
          public int compare(OJPAPersistenceUnitInfo p1, OJPAPersistenceUnitInfo p2) {
            return Integer.valueOf(p1.getPersistenceUnitName())
                .compareTo(Integer.valueOf(p2.getPersistenceUnitName()));
          }
        });

    for (int counter = 1; counter < units.size(); counter++) {
      assertPersistenceUnit(
          units.get(counter - 1),
          new Rule[] { //
            new Rule("The schema version was incorrect", "1.0", "getPersistenceXMLSchemaVersion"),
            new Rule(
                "The unit name was incorrect", Integer.toString(counter), "getPersistenceUnitName"),
            new Rule("The transaction type was incorrect", JTA, "getTransactionType"),
            new Rule(
                "The provider class name was incorrect",
                "provider." + counter,
                "getPersistenceProviderClassName"),
            new Rule(
                "Incorrect mapping files were listed",
                asList("mappingFile." + counter),
                "getMappingFileNames"),
            new Rule(
                "Incorrect jar URLs were listed",
                asURLList("jarFile." + counter),
                "getJarFileUrls"),
            new Rule(
                "Incorrect managed classes were listed",
                asList("class." + counter),
                "getManagedClassNames"),
            new Rule("We should not exclude any classes", true, "excludeUnlistedClasses"),
            new Rule(
                "The properties should never be null",
                asProperty("some.prop." + counter, "prop.value." + counter),
                "getProperties")
          });
    }
  }

  // ---------------- helpers

  @Test
  public void elementsPrefixedWithPersistenceNameSpaceShouldBeAccepted() throws Exception {
    URL locationUrl =
        getClass().getClassLoader().getResource("apache-aries/file26/META-INF/persistence.xml");
    Collection<? extends PersistenceUnitInfo> parsedUnits = PersistenceXmlUtil.parse(locationUrl);

    assertNotNull("Persistence units shouldn't be null.", parsedUnits);
    assertEquals(
        "An incorrect number of persistence units has been returned.", 1, parsedUnits.size());
  }

  @Test(expected = PersistenceException.class)
  public void elementsPrefixedWithWrongNameSpaceShouldBeRejected() throws Exception {
    URL locationUrl =
        getClass().getClassLoader().getResource("apache-aries/file27/META-INF/persistence.xml");
    PersistenceXmlUtil.parse(locationUrl);

    fail("should throw");
  }

  @Test(expected = PersistenceException.class)
  public void testConfigWithoutXMLSchemaVersion() throws Exception {
    URL locationUrl =
        getClass().getClassLoader().getResource("orient/file1/META-INF/persistence.xml");
    PersistenceXmlUtil.parse(locationUrl);

    fail("should throw");
  }

  class Rule {
    public String message;
    public Object expected;
    public String method;

    public Rule(String message, Object expected, String method) {
      this.message = message;
      this.expected = expected;
      this.method = method;
    }
  }
}
