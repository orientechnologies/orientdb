/*
 *
 *  * Copyright 2010-2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.orient.etl.extractor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.OETLProcessor;
import com.orientechnologies.orient.etl.OExtractedItem;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

public class OXmlExtractor extends OAbstractSourceExtractor {
  protected OExtractedItem next;
  private boolean attributesAsNodes = false;
  private int     skipFirstLevels   = 0;

  @Override
  public void configure(OETLProcessor iProcessor, ODocument iConfiguration, OCommandContext iContext) {
    super.configure(iProcessor, iConfiguration, iContext);

    if (iConfiguration.containsField("attributesAsNodes"))
      attributesAsNodes = (Boolean) iConfiguration.field("attributesAsNodes");

    if (iConfiguration.containsField("skipFirstLevels"))
      skipFirstLevels = (Integer) iConfiguration.field("skipFirstLevels");
  }

  @Override
  public boolean hasNext() {
    return next != null;
  }

  @Override
  public OExtractedItem next() {
    if (!hasNext())
      throw new NoSuchElementException("EOF");

    try {
      return next;
    } finally {
      next = null;
    }
  }

  @Override
  public void extract(final Reader iReader) {
    super.extract(iReader);

    try {
      final DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
      final DocumentBuilder dBuilder;
      dBuilder = dbFactory.newDocumentBuilder();

      final InputSource is = new InputSource(iReader);
      final Document xmlDocument = dBuilder.parse(is);
      xmlDocument.getDocumentElement().normalize();

      final ODocument doc = xml2doc(xmlDocument);
      next = new OExtractedItem(current++, doc);

    } catch (ParserConfigurationException e) {
      throw new OExtractorException("[XML extractor] error on creating XML parser", e);
    } catch (Exception e) {
      throw new OExtractorException("[XML extractor] error on parsing XML", e);
    }

  }

  private ODocument xml2doc(final Node xmlDocument) {
    return xml2doc(xmlDocument, 0);
  }

  private ODocument xml2doc(final Node xmlDocument, final int iLevel) {
    final ODocument doc = new ODocument();

    final NamedNodeMap attrs = xmlDocument.getAttributes();
    if (attrs != null)
      for (int i = 0; i<attrs.getLength(); ++i) {
        final Node item = attrs.item(i);
        switch (item.getNodeType()) {
        case Node.ATTRIBUTE_NODE: {
          final Attr attr = (Attr) item;
          doc.field(attr.getName(), attr.getValue());
          break;
        }
        }
      }

    final NodeList children = xmlDocument.getChildNodes();
    if (children != null)
      for (int i = 0; i<children.getLength(); ++i) {
        final Node child = children.item(i);
        switch (child.getNodeType()) {
        case Node.ELEMENT_NODE: {
          final Element element = (Element) child;
          ODocument linked = xml2doc(element, iLevel + 1);

          final Object previous = doc.field(element.getNodeName());
          if (previous != null) {
            List list;
            if (previous instanceof List) {
              list = (List) previous;
            } else {
              // TRANSFORM IN A LIST
              list = new ArrayList();
              list.add(previous);
              doc.field(element.getNodeName(), list, OType.EMBEDDEDLIST);
            }
            list.add(linked);
          } else
            doc.field(element.getNodeName(), linked, OType.EMBEDDED);

          break;
        }
        }
      }
    return doc;
  }

  @Override
  public ODocument getConfiguration() {
    return new ODocument().fromJSON("{parameters:[],output:'ODocument'}");
  }

  @Override
  public String getUnit() {
    return "entries";
  }

  @Override
  public String getName() {
    return "xml";
  }

}
