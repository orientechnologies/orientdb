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

package com.orientechnologies.orient.object.jpa;

import com.orientechnologies.orient.object.jpa.parsing.JPAVersion;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import javax.persistence.PersistenceException;
import javax.persistence.SharedCacheMode;
import javax.persistence.ValidationMode;
import javax.persistence.spi.ClassTransformer;
import javax.persistence.spi.PersistenceUnitInfo;
import javax.persistence.spi.PersistenceUnitTransactionType;
import javax.sql.DataSource;

/** An implementation of PersistenceUnit for parsed persistence unit metadata */
public class OJPAPersistenceUnitInfo implements PersistenceUnitInfo {
  /** the name of the persistence unit */
  private final String unitName;

  /** transaction type of the entity managers created by the EntityManagerFactory */
  private final PersistenceUnitTransactionType transactionType;

  /**
   * The JAR file or directory whose META-INF directory contains persistence.xml is called the root
   * of the persistence unit. The scope of the persistence unit is determined by the persistence
   * unit's root.
   */
  private final URL unitRootUrl;
  /**
   * the list of mapping file names that the persistence provider must load to determine the
   * mappings for the entity classes
   */
  private final List<String> mappingFileNames = new ArrayList<String>();

  /**
   * the list of the names of the classes that the persistence provider must add to its set of
   * managed classes
   */
  private final List<String> managedClassNames = new ArrayList<String>();

  /**
   * whether classes in the root of the persistence unit that have not been explicitly listed are to
   * be included in the set of managed classes. When set to true then only listed classes and jars
   * will be scanned for persistent classes, otherwise the enclosing jar or directory will also be
   * scanned. Not applicable to Java SE persistence units.
   *
   * @see 'Note'
   *     http://static.springsource.org/spring/docs/4.0.x/spring-framework-reference/html/orm.html#orm-jpa-setup-lcemfb
   *     The exclude-unlisted-classes element always indicates that no scanning for annotated entity
   *     classes is supposed to occur, in order to support the <exclude-unlisted-classes/> shortcut.
   *     This is in line with the JPA specification, which suggests that shortcut, but unfortunately
   *     is in conflict with the JPA XSD, which implies false for that shortcut. Consequently,
   *     <exclude-unlisted-classes> false </exclude-unlisted-classes/> is not supported. Simply omit
   *     the exclude-unlisted-classes element if you want entity class scanning to occur.
   */
  private boolean excludeUnlistedClasses = false;

  /** the second-level cache mode that must be used by the provider for the persistence unit */
  private SharedCacheMode sharedCacheMode = SharedCacheMode.UNSPECIFIED;

  /** the validation mode to be used by the persistence provider for the persistence unit */
  private ValidationMode validationMode = ValidationMode.AUTO;

  /** OrientDB Properties object */
  private final Properties properties = new OJPAProperties();

  /**
   * TODO: implement transformer provider-supplied transformer that the container invokes at
   * class-(re)definition time
   */
  private final Set<ClassTransformer> classTransformers = new HashSet<ClassTransformer>();

  private final List<URL> jarFileUrls = new ArrayList<URL>();

  private String providerClassName;

  private final JPAVersion xmlSchemaVersion;

  /**
   * Create a new persistence unit with the given name, transaction type, location and defining
   * bundle
   *
   * @param unitName must not be null
   * @param transactionType may be null
   * @param unitRootUrl root of the persistence unit
   * @param schemaVersion The version of the JPA schema used in persistence.xml
   */
  public OJPAPersistenceUnitInfo(
      String unitName, String transactionType, URL unitRootUrl, String xmlSchemaVersion) {
    this.unitName = unitName;
    this.unitRootUrl = unitRootUrl;
    if (unitName == null || unitName.isEmpty()) {
      throw new IllegalStateException(
          "PersistenceUnitName for entity manager should not be null or empty");
    }
    this.xmlSchemaVersion = JPAVersion.parse(xmlSchemaVersion);
    this.transactionType = initTransactionType(transactionType);
  }

  /** @param provider */
  public void setProviderClassName(String providerClassName) {
    this.providerClassName = providerClassName;
  }

  /** @param jtaDataSource */
  public void setJtaDataSource(String jtaDataSource) {
    // TODO: implement
  }

  /** @param nonJtaDataSource */
  public void setNonJtaDataSource(String nonJtaDataSource) {
    // TODO: implement
  }

  /** @param mappingFileName */
  public void addMappingFileName(String mappingFileName) {
    mappingFileNames.add(mappingFileName);
  }

  /** @param jarFileName */
  public void addJarFileName(String jarFileName) {
    jarFileUrls.add(initJarFile(jarFileName));
  }

  /** @param className */
  public void addClassName(String className) {
    managedClassNames.add(className);
  }

  /** @param exclude */
  public void setExcludeUnlisted(boolean exclude) {
    excludeUnlistedClasses = exclude;
  }

  /**
   * @param name
   * @param value
   */
  public void addProperty(String name, String value) {
    properties.setProperty(name, value);
  }

  /** @param sharedCacheMode */
  public void setSharedCacheMode(String sharedCacheMode) {
    this.sharedCacheMode = initSharedCacheMode(sharedCacheMode);
  }

  /** @param validationMode */
  public void setValidationMode(String validationMode) {
    this.validationMode = initValidationMode(validationMode);
  }

  @Override
  public String toString() {
    return "PersistenceUnit@" + unitName + " " + super.toString();
  }

  @Override
  public String getPersistenceUnitName() {
    return unitName;
  }

  @Override
  public String getPersistenceProviderClassName() {
    return providerClassName;
  }

  @Override
  public PersistenceUnitTransactionType getTransactionType() {
    return transactionType;
  }

  @Override
  public DataSource getJtaDataSource() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DataSource getNonJtaDataSource() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<String> getMappingFileNames() {
    return mappingFileNames;
  }

  @Override
  public List<URL> getJarFileUrls() {
    return jarFileUrls;
  }

  @Override
  public URL getPersistenceUnitRootUrl() {
    return unitRootUrl;
  }

  @Override
  public List<String> getManagedClassNames() {
    return managedClassNames;
  }

  @Override
  public boolean excludeUnlistedClasses() {
    return excludeUnlistedClasses;
  }

  @Override
  public SharedCacheMode getSharedCacheMode() {
    return sharedCacheMode;
  }

  @Override
  public ValidationMode getValidationMode() {
    return validationMode;
  }

  @Override
  public Properties getProperties() {
    return properties;
  }

  @Override
  public String getPersistenceXMLSchemaVersion() {
    return xmlSchemaVersion.getVersion();
  }

  @Override
  public ClassLoader getClassLoader() {
    return ThreadLocal.class.getClassLoader();
  }

  @Override
  public void addTransformer(ClassTransformer transformer) {
    classTransformers.add(transformer);
  }

  @Override
  public ClassLoader getNewTempClassLoader() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int hashCode() {
    return unitName.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    return unitName.equals(((OJPAPersistenceUnitInfo) obj).getPersistenceUnitName());
  }

  // ------------- helpers

  /**
   * TODO: init default value In a Java EE environment, if this element is not specified, the
   * default is JTA. In a Java SE environment, if this element is not specified, a default of
   * RESOURCE_LOCAL may be assumed.
   *
   * @param elementContent
   * @return
   */
  public static PersistenceUnitTransactionType initTransactionType(String elementContent) {
    if (elementContent == null || elementContent.isEmpty()) {
      return null;
    }

    try {
      return PersistenceUnitTransactionType.valueOf(elementContent.toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException ex) {
      throw new PersistenceException("Unknown TransactionType: " + elementContent, ex);
    }
  }

  public static ValidationMode initValidationMode(String validationMode) {
    if (validationMode == null || validationMode.isEmpty()) {
      return ValidationMode.AUTO;
    }

    try {
      return ValidationMode.valueOf(validationMode.toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException ex) {
      throw new PersistenceException("Unknown ValidationMode: " + validationMode, ex);
    }
  }

  public static SharedCacheMode initSharedCacheMode(String sharedCacheMode) {
    if (sharedCacheMode == null || sharedCacheMode.isEmpty()) {
      return SharedCacheMode.UNSPECIFIED;
    }

    try {
      return SharedCacheMode.valueOf(sharedCacheMode.toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException ex) {
      throw new PersistenceException("Unknown ValidationMode: " + sharedCacheMode, ex);
    }
  }

  public static URL initJarFile(String jarFileName) {
    try {
      return new URL("file://" + jarFileName);
    } catch (MalformedURLException e) {
      throw new PersistenceException("Unknown jar file name: " + jarFileName, e);
    }
  }
}
