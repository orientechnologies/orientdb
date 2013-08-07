/*
 *
 * Copyright 2013 Luca Molino (molino.luca--AT--gmail.com)
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
package com.orientechnologies.orient.object.enhancement;

import java.lang.reflect.Field;
import java.util.List;

import com.orientechnologies.common.reflection.OReflectionHelper;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;

/**
 * @author luca.molino
 *
 */
public class OObjectSchemaGenerator {

  /**
   * Generate/updates the SchemaClass and properties from given Class<?>.
   * 
   * @param iClass
   *          :- the Class<?> to generate
   */
  public static synchronized void generateSchema(final Class<?> iClass, ODatabaseRecord database) {
    if (iClass == null || iClass.isInterface() || iClass.isPrimitive() || iClass.isEnum() || iClass.isAnonymousClass())
      return;
    OObjectEntitySerializer.registerClass(iClass);
    List<String> fields = OObjectEntitySerializer.allFields.get(iClass);
    OClass schema = database.getMetadata().getSchema().getClass(iClass);
    for (String field : fields) {
      if (schema.existsProperty(field))
        continue;
      if (OObjectEntitySerializer.isVersionField(iClass, field) || OObjectEntitySerializer.isIdField(iClass, field))
        continue;
      Field f = OObjectEntitySerializer.getField(field, iClass);
      if (f.getType().equals(Object.class) || f.getType().equals(ODocument.class) || f.getType().equals(ORecordBytes.class)) {
        continue;
      }
      OType t = OObjectEntitySerializer.getTypeByClass(iClass, field, f);
      if (t == null) {
        if (f.getType().isEnum())
          t = OType.STRING;
        else {
          t = OType.LINK;
        }
      }
      switch (t) {
  
      case LINK:
        Class<?> linkedClazz = f.getType();
        OObjectSchemaGenerator.generateLinkProperty(database, schema, field, t, linkedClazz);
        break;
      case LINKLIST:
      case LINKMAP:
      case LINKSET:
        linkedClazz = OReflectionHelper.getGenericMultivalueType(f);
        if (linkedClazz != null)
          OObjectSchemaGenerator.generateLinkProperty(database, schema, field, t, linkedClazz);
        break;
  
      case EMBEDDED:
        linkedClazz = f.getType();
        if (linkedClazz == null || linkedClazz.equals(Object.class) || linkedClazz.equals(ODocument.class)
            || f.getType().equals(ORecordBytes.class)) {
          continue;
        } else {
          OObjectSchemaGenerator.generateLinkProperty(database, schema, field, t, linkedClazz);
        }
        break;
  
      case EMBEDDEDLIST:
      case EMBEDDEDSET:
      case EMBEDDEDMAP:
        linkedClazz = OReflectionHelper.getGenericMultivalueType(f);
        if (linkedClazz == null || linkedClazz.equals(Object.class) || linkedClazz.equals(ODocument.class)
            || f.getType().equals(ORecordBytes.class)) {
          continue;
        } else {
          if (OReflectionHelper.isJavaType(linkedClazz)) {
            schema.createProperty(field, t, OType.getTypeByClass(linkedClazz));
          } else if (linkedClazz.isEnum()) {
            schema.createProperty(field, t, OType.STRING);
          } else {
            OObjectSchemaGenerator.generateLinkProperty(database, schema, field, t, linkedClazz);
          }
        }
        break;
  
      default:
        schema.createProperty(field, t);
        break;
      }
    }
  }

  protected static void generateLinkProperty(ODatabaseRecord database, OClass schema, String field, OType t, Class<?> linkedClazz) {
    OClass linkedClass = database.getMetadata().getSchema().getClass(linkedClazz);
    if (linkedClass == null) {
      OObjectEntitySerializer.registerClass(linkedClazz);
      linkedClass = database.getMetadata().getSchema().getClass(linkedClazz);
    }
    schema.createProperty(field, t, linkedClass);
  }

}
