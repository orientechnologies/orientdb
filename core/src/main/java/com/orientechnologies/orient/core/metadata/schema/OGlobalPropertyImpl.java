/*
  *
  *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
  *  * For more information: http://www.orientechnologies.com
  *
  */

package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.ODocumentSerializable;

public class OGlobalPropertyImpl implements OGlobalProperty, ODocumentSerializable {

  private String  name;
  private OType   type;
  private Integer id;

  public OGlobalPropertyImpl() {
  }

  public OGlobalPropertyImpl(final String name, final OType type, final Integer id) {
    this.name = name;
    this.type = type;
    this.id = id;
  }

  public Integer getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public OType getType() {
    return type;
  }

  @Override
  public void fromDocument(final ODocument document) {
    this.name = document.field("name");
    this.type = OType.valueOf((String) document.field("type"));
    this.id = document.field("id");
  }

  @Override
  public ODocument toDocument() {
    final ODocument doc = new ODocument();
    doc.field("name", name);
    doc.field("type", type.name());
    doc.field("id", id);
    return doc;
  }
}
