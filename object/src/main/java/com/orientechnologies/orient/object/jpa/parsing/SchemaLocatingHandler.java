package com.orientechnologies.orient.object.jpa.parsing;

import static com.orientechnologies.orient.object.jpa.parsing.PersistenceXml.TAG_PERSISTENCE;

import java.util.EnumSet;
import javax.persistence.PersistenceException;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * This parser provides a quick mechanism for determining the JPA schema level, and throws an
 * StopSAXParser with the Schema to validate with
 */
class SchemaLocatingHandler extends DefaultHandler {

  /** The value of the version attribute in the xml */
  private String schemaVersion = "";

  /** Create a new SchemaLocatingHandler */
  public SchemaLocatingHandler() {}

  /**
   * Fist tag of 'persistence.xml' (<persistence>) have to have 'version' attribute
   *
   * @see DefaultHandler#startElement(String, String, String, org.xml.sax.Attributes)
   */
  @Override
  public void startElement(String uri, String localName, String name, Attributes attributes)
      throws SAXException {
    PersistenceXml element =
        PersistenceXml.parse((localName == null || localName.isEmpty()) ? name : localName);
    schemaVersion = PersistenceXmlUtil.parseSchemaVersion(uri, element, attributes);
    // found, stop parsing
    if (schemaVersion != null) {
      throw new StopSAXParser();
    }

    // This should never occurs, however check if contain known tag other than TAG_PERSISTENCE
    if (TAG_PERSISTENCE != element && EnumSet.allOf(PersistenceXml.class).contains(element)) {
      throw new PersistenceException("Cannot find schema version attribute in <persistence> tag");
    }
  }

  /** @return The version of the JPA schema used */
  public String getVersion() {
    return schemaVersion;
  }
}
