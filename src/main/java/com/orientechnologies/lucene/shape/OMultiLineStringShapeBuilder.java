/*
 *
 *  * Copyright 2014 Orient Technologies.
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

package com.orientechnologies.lucene.shape;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchemaProxy;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.spatial4j.core.shape.Shape;

public class OMultiLineStringShapeBuilder extends OShapeBuilder {
  @Override
  public String getName() {
    return "MultiLineString";
  }

  @Override
  public OShapeType getType() {
    return OShapeType.MULTILINESTRING;
  }

  @Override
  public void initClazz(ODatabaseDocumentTx db) {
    OSchemaProxy schema = db.getMetadata().getSchema();
    OClass lineString = schema.createClass(getName());
    lineString.createProperty(COORDINATES, OType.EMBEDDEDLIST, OType.EMBEDDEDLIST);
  }

  @Override
  public String asText(Shape shape) {
    return null;
  }

  @Override
  public Shape fromText(String wkt) {
    return null;
  }

  @Override
  public ODocument toDoc(Shape shape) {
    return null;
  }
}
