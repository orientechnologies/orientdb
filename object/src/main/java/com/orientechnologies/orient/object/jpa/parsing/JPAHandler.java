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

package com.orientechnologies.orient.object.jpa.parsing;

import com.orientechnologies.orient.object.jpa.OJPAPersistenceUnitInfo;
import java.net.URL;
import java.util.Collection;
import java.util.Stack;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

/** This code is responsible for parsing the persistence.xml into PersistenceUnits */
public class JPAHandler extends DefaultHandler {
  /** The Persistence Units that we have parsed */
  private final Stack<OJPAPersistenceUnitInfo> persistenceUnits =
      new Stack<OJPAPersistenceUnitInfo>();
  /** root of the persistence unit */
  private final URL persistenceXmlRoot;
  /** The version of the persistence.xml file */
  private String xmlSchemaVersion;
  /** The name of the current element */
  private PersistenceXml element;
  /** A StringBuilder for caching the information from getCharacters */
  private StringBuilder builder = new StringBuilder();

  // /**
  // * Create a new JPA Handler for the given persistence.xml. If no xmlSchemaVersion defined it
  // will be parsed
  // */
  // public JPAHandler() {
  // }

  /**
   * Create a new JPA Handler for the given persistence.xml
   *
   * @param persistenceXmlRoot
   * @param jpaVersion the version of the JPA schema used in the xml
   */
  public JPAHandler(URL persistenceXmlRoot, JPAVersion jpaVersion) {
    this.persistenceXmlRoot = persistenceXmlRoot;
    this.xmlSchemaVersion = jpaVersion.getVersion();
  }

  @Override
  public void startElement(String uri, String localName, String name, Attributes attributes)
      throws SAXException {
    // Do this setting first as we use it later.
    element = PersistenceXml.parse((localName == null || localName.isEmpty()) ? name : localName);

    switch (element) {
      case TAG_PERSISTENCE:
        if (xmlSchemaVersion == null) {
          xmlSchemaVersion = PersistenceXmlUtil.parseSchemaVersion(uri, element, attributes);
        }
        break;
      case TAG_PERSISTENCE_UNIT:
        String unitName = attributes.getValue(PersistenceXml.ATTR_UNIT_NAME.toString());
        String transactionType =
            attributes.getValue(PersistenceXml.ATTR_TRANSACTION_TYPE.toString());
        persistenceUnits.push(
            new OJPAPersistenceUnitInfo(
                unitName, transactionType, persistenceXmlRoot, xmlSchemaVersion));
        break;
      case TAG_EXCLUDE_UNLISTED_CLASSES:
        persistenceUnits.peek().setExcludeUnlisted(true);
        break;
      case TAG_PROPERTY:
        persistenceUnits
            .peek()
            .addProperty(attributes.getValue("name"), attributes.getValue("value"));
        break;
      default:
    }
  }

  @Override
  public void endElement(String uri, String localName, String name) throws SAXException {
    String s = builder.toString().trim();
    // This step is VERY important, otherwise we pollute subsequent
    // ELEMENTS
    builder = new StringBuilder();

    if (s.isEmpty()) {
      return;
    }

    OJPAPersistenceUnitInfo pu = persistenceUnits.peek();

    switch (element) {
      case TAG_PROVIDER:
        pu.setProviderClassName(s);
        break;
      case TAG_JTA_DATA_SOURCE:
        pu.setJtaDataSource(s);
        break;
      case TAG_NON_JTA_DATA_SOURCE:
        pu.setNonJtaDataSource(s);
        break;
      case TAG_MAPPING_FILE:
        pu.addMappingFileName(s);
        break;
      case TAG_JAR_FILE:
        pu.addJarFileName(s);
        break;
      case TAG_CLASS:
        pu.addClassName(s);
        break;
      case TAG_EXCLUDE_UNLISTED_CLASSES:
        pu.setExcludeUnlisted(Boolean.parseBoolean(s));
        break;
      case TAG_SHARED_CACHE_MODE:
        pu.setSharedCacheMode(s);
        break;
      case TAG_VALIDATION_MODE:
        pu.setValidationMode(s);
        break;
      default:
    }
  }

  /**
   * Collect up the characters, as element's characters may be split across multiple calls. Isn't
   * SAX lovely...
   */
  @Override
  public void characters(char[] ch, int start, int length) throws SAXException {
    builder.append(ch, start, length);
  }

  /** We throw this exception to be caught further up and logged as an error there */
  @Override
  public void error(SAXParseException spe) throws SAXException {
    throw spe;
  }

  /** @return The collection of persistence units that we have parsed */
  public Collection<OJPAPersistenceUnitInfo> getPersistenceUnits() {
    return persistenceUnits;
  }
}
