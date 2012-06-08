/*
 *
 * Copyright 2012 Luca Molino (molino.luca--AT--gmail.com)
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
package com.orientechnologies.orient.core.serialization.serializer.object;

import com.orientechnologies.orient.core.annotation.ODocumentInstance;
import com.orientechnologies.orient.core.db.OUserObject2RecordHandler;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.entity.OEntityManager;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * @author luca.molino
 * 
 */
public class OObjectSerializerHelperDocument implements OObjectSerializerHelperInterface {

  private Set<String>              classes             = new HashSet<String>();
  private HashMap<Class<?>, Field> boundDocumentFields = new HashMap<Class<?>, Field>();

  public ODocument toStream(Object iPojo, ODocument iRecord, OEntityManager iEntityManager, OClass schemaClass,
      OUserObject2RecordHandler iObj2RecHandler, ODatabaseObject db, boolean iSaveOnlyDirty) {
    return null;
  }

  public String getDocumentBoundField(Class<?> iClass) {
    getClassFields(iClass);
    final Field f = boundDocumentFields.get(iClass);
    return f != null ? f.getName() : null;
  }

  public Object getFieldValue(Object iPojo, String iProperty) {
    return null;
  }

  public void invokeCallback(Object iPojo, ODocument iDocument, Class<?> iAnnotation) {
  }

  private void getClassFields(final Class<?> iClass) {
    if (iClass.getName().startsWith("java.lang"))
      return;

    synchronized (classes) {
      if (classes.contains(iClass.getName()))
        return;

      analyzeClass(iClass);
    }
  }

  protected void analyzeClass(final Class<?> iClass) {
    classes.add(iClass.getName());

    int fieldModifier;

    for (Class<?> currentClass = iClass; currentClass != Object.class;) {
      for (Field f : currentClass.getDeclaredFields()) {
        fieldModifier = f.getModifiers();
        if (Modifier.isStatic(fieldModifier) || Modifier.isNative(fieldModifier) || Modifier.isTransient(fieldModifier))
          continue;

        if (f.getName().equals("this$0"))
          continue;

        // CHECK FOR AUTO-BINDING
        if (f.getAnnotation(ODocumentInstance.class) != null)
          // BOUND DOCUMENT ON IT
          boundDocumentFields.put(iClass, f);
      }
      currentClass = currentClass.getSuperclass();

      if (currentClass.equals(ODocument.class))
        // POJO EXTENDS ODOCUMENT: SPECIAL CASE: AVOID TO CONSIDER
        // ODOCUMENT FIELDS
        break;
    }
  }

}
