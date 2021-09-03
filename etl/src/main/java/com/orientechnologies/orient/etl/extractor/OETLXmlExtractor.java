/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (info(-at-)orientdb.com)
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
import com.orientechnologies.orient.etl.OETLExtractedItem;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

public class OETLXmlExtractor extends OETLAbstractSourceExtractor {
  protected List items = new ArrayList();
  private Collection<String> tagsAsAttribute = new HashSet<String>();
  private String rootNode;

  @Override
  public void configure(ODocument iConfiguration, OCommandContext iContext) {
    super.configure(iConfiguration, iContext);

    if (iConfiguration.containsField("rootNode")) rootNode = iConfiguration.field("rootNode");

    if (iConfiguration.containsField("tagsAsAttribute"))
      tagsAsAttribute = iConfiguration.<Collection<String>>field("tagsAsAttribute");
  }

  @Override
  public boolean hasNext() {
    return current < items.size();
  }

  @Override
  public OETLExtractedItem next() {
    if (!hasNext()) throw new NoSuchElementException("EOF");

    return new OETLExtractedItem(current, items.get((int) current++));
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

      final Object res = xml2doc(xmlDocument);
      if (res instanceof Collection) items.addAll((Collection) res);
      else items.add(res);

    } catch (ParserConfigurationException e) {
      throw new OETLExtractorException("[XML extractor] error on creating XML parser", e);
    } catch (Exception e) {
      throw new OETLExtractorException("[XML extractor] error on parsing XML", e);
    }
  }

  private Object xml2doc(final Node xmlDocument) {
    return xml2doc(xmlDocument, "", 0);
  }

  private Object xml2doc(final Node xmlDocument, final String iPath, final int iLevel) {
    final ODocument doc = new ODocument();
    Object result = doc;

    final NamedNodeMap attrs = xmlDocument.getAttributes();
    if (attrs != null)
      for (int i = 0; i < attrs.getLength(); ++i) {
        final Node item = attrs.item(i);
        switch (item.getNodeType()) {
          case Node.ATTRIBUTE_NODE:
            {
              final Attr attr = (Attr) item;
              doc.field(attr.getName(), attr.getValue());
              break;
            }
        }
      }

    final NodeList children = xmlDocument.getChildNodes();
    if (children != null) {
      for (int i = 0; i < children.getLength(); ++i) {
        final Node child = children.item(i);
        switch (child.getNodeType()) {
          case Node.ELEMENT_NODE:
            {
              final Element element = (Element) child;

              final String path =
                  iPath.isEmpty() ? element.getNodeName() : iPath + "." + element.getNodeName();

              if (tagsAsAttribute.contains(iPath)) {

                final NodeList subChildren = element.getChildNodes();
                if (subChildren.getLength() > 0) {
                  final Node fieldContent = subChildren.item(0);
                  doc.field(element.getNodeName(), fieldContent.getTextContent());
                }

              } else {

                final Object sub = xml2doc(element, path, iLevel + 1);

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
                  list.add(sub);
                } else doc.field(element.getNodeName(), sub, OType.EMBEDDED);

                if (rootNode != null && rootNode.startsWith(path))
                  // SKIP
                  result = doc.field(element.getNodeName());
              }

              break;
            }
        }
      }
    }

    return result;
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
