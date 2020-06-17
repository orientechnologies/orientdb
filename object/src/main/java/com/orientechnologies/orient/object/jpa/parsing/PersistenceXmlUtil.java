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

import static com.orientechnologies.orient.object.jpa.parsing.PersistenceXml.ATTR_SCHEMA_VERSION;
import static com.orientechnologies.orient.object.jpa.parsing.PersistenceXml.TAG_PERSISTENCE;
import static javax.xml.XMLConstants.W3C_XML_SCHEMA_NS_URI;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collection;
import javax.persistence.PersistenceException;
import javax.persistence.spi.PersistenceUnitInfo;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/** Simple handler for persistence.xml files. */
public final class PersistenceXmlUtil {
  /** URI for the JPA persistence namespace */
  public static final String PERSISTENCE_NS_URI = "http://java.sun.com/xml/ns/persistence";

  private static final SchemaFactory schemaFactory =
      SchemaFactory.newInstance(W3C_XML_SCHEMA_NS_URI);
  private static final SAXParserFactory parserFactory = SAXParserFactory.newInstance();

  static {
    parserFactory.setNamespaceAware(true);
  }

  /** The persistence xml root */
  public static final String PERSISTENCE_XML_ROOT = "META-INF/";

  public static final String PERSISTENCE_XML_BASE_NAME = "persistence.xml";
  /** The persistence XSD location */
  public static final String PERSISTENCE_XSD_DIR = PERSISTENCE_XML_ROOT + "persistence/";
  /** The persistence XML location */
  public static final String PERSISTENCE_XML = PERSISTENCE_XML_ROOT + PERSISTENCE_XML_BASE_NAME;

  private PersistenceXmlUtil() {}

  /**
   * Parse the persistence.xml files referenced by the URLs in the collection
   *
   * @param persistenceXml
   * @return A collection of parsed persistence units, or null if nothing has been found
   */
  public static PersistenceUnitInfo findPersistenceUnit(
      String unitName, Collection<? extends PersistenceUnitInfo> units) {
    if (units == null || unitName == null) {
      return null;
    }

    for (PersistenceUnitInfo unit : units) {
      if (unitName.equals(unit.getPersistenceUnitName())) {
        return unit;
      }
    }
    return null;
  }

  /**
   * Parse the persistence.xml files referenced by the URLs in the collection
   *
   * @param persistenceXml
   * @return A collection of parsed persistence units.
   * @throws IOException
   * @throws SAXException
   * @throws ParserConfigurationException
   */
  public static Collection<? extends PersistenceUnitInfo> parse(URL persistenceXml) {

    InputStream is = null;
    try {
      // Buffer the InputStream so we can mark it, though we'll be in
      // trouble if we have to read more than 8192 characters before finding
      // the schema!
      is = new BufferedInputStream(persistenceXml.openStream());

      JPAVersion jpaVersion = getSchemaVersion(is);
      Schema schema = getSchema(jpaVersion);

      if (schema == null) {
        throw new PersistenceException("Schema is unknown");
      }

      // Get back to the beginning of the stream
      is = new BufferedInputStream(persistenceXml.openStream());

      parserFactory.setNamespaceAware(true);

      int endIndex = persistenceXml.getPath().length() - PERSISTENCE_XML_BASE_NAME.length();
      URL persistenceXmlRoot = new URL("file://" + persistenceXml.getFile().substring(0, endIndex));

      return getPersistenceUnits(is, persistenceXmlRoot, jpaVersion);
    } catch (Exception e) {
      throw new PersistenceException("Something goes wrong while parsing persistence.xml", e);
    } finally {
      if (is != null)
        try {
          is.close();
        } catch (IOException e) {
          // No logging necessary, just consume
        }
    }
  }

  public static Schema getSchema(JPAVersion version) throws SAXException {
    String schemaPath = PERSISTENCE_XSD_DIR + version.getFilename();
    InputStream inputStream =
        PersistenceXmlUtil.class.getClassLoader().getResourceAsStream(schemaPath);
    return schemaFactory.newSchema(new StreamSource(inputStream));
  }

  public static JPAVersion getSchemaVersion(InputStream is)
      throws ParserConfigurationException, SAXException, IOException {
    SchemaLocatingHandler schemaHandler = parse(is, new SchemaLocatingHandler());
    return JPAVersion.parse(schemaHandler.getVersion());
  }

  public static Collection<? extends PersistenceUnitInfo> getPersistenceUnits(
      InputStream is, URL xmlRoot, JPAVersion version)
      throws ParserConfigurationException, SAXException, IOException {
    JPAHandler handler = new JPAHandler(xmlRoot, version);
    return parse(is, handler).getPersistenceUnits();
  }

  /**
   * @param is - xml file to be validated
   * @param handler
   * @return handler for chained calls
   * @throws ParserConfigurationException
   * @throws SAXException
   * @throws IOException
   */
  protected static <T extends DefaultHandler> T parse(InputStream is, T handler)
      throws ParserConfigurationException, SAXException, IOException {
    try {
      SAXParser parser = parserFactory.newSAXParser();
      parser.parse(is, handler);
    } catch (StopSAXParser e) {
      // This is not really an exception, but a way to work out which
      // version of the persistence schema to use in validation
    }
    return handler;
  }

  /**
   * @param uri
   * @param element
   * @param attributes
   * @return XML Schema Version or null
   * @throws SAXException
   */
  public static String parseSchemaVersion(String uri, PersistenceXml element, Attributes attributes)
      throws SAXException {
    if (PERSISTENCE_NS_URI.equals(uri) && TAG_PERSISTENCE == element) {
      return attributes.getValue(ATTR_SCHEMA_VERSION.toString());
    }
    return null;
  }
}
