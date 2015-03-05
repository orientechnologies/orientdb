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
package com.orientechnologies.orient.core.processor.block;

import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.processor.OComposableProcessor;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OLetBlock extends OAbstractBlock {
  public static final String NAME = "let";

  @SuppressWarnings("unchecked")
  @Override
  public Object processBlock(OComposableProcessor iManager, final OCommandContext iContext, final ODocument iConfig,
      ODocument iOutput, final boolean iReadOnly) {
    final Boolean copy = getFieldOfClass(iContext, iConfig, "copy", Boolean.class);
    final Boolean flatMultivalues = getFieldOfClass(iContext, iConfig, "flatMultivalues", Boolean.class);

    Object target = getField(iContext, iConfig, "target");
    if (target != null && target.equals("null")) {
        target = new ODocument();
    }

    final Object value = getRawField(iConfig, "value");
    if (value != null) {
      if (value instanceof ODocument) {
        final ODocument doc = ((ODocument) value);
        for (String fieldName : doc.fieldNames()) {
          final Object v = resolveValue(iContext, doc.field(fieldName), true);
          if (target != null) {
            debug(iContext, "Set value %s in document field '%s'", v, fieldName);
            ((ODocument) target).field(fieldName, v);
          } else {
              assignVariable(iContext, fieldName, v);
          }
        }
      } else if (value instanceof Map<?, ?>) {
        for (Entry<Object, Object> entry : ((Map<Object, Object>) value).entrySet()) {
          final Object v = resolveValue(iContext, getValue(entry.getValue(), copy), true);
          if (target != null) {
            debug(iContext, "Set value %s in document field '%s'", v, entry.getKey());
            ((ODocument) target).field(entry.getKey().toString(), v);
          } else {
              assignVariable(iContext, entry.getKey().toString(), v);
          }
        }

      } else {
        final String name = getRequiredFieldOfClass(iContext, iConfig, "name", String.class);
        Object v = getValue(getRequiredField(iContext, iConfig, "value"), copy);

        v = flatMultivalues(iContext, copy, flatMultivalues, v);

        if (target != null) {
          if (OMultiValue.isMultiValue(v)) {
            for (int i = 0; i < OMultiValue.getSize(v); ++i) {
              final Object fieldName = OMultiValue.getValue(v, i);

              if (fieldName != null && !((ODocument) target).containsField(fieldName.toString())) {
                debug(iContext, "Set value %s in document field '%s'", null, fieldName);
                ((ODocument) target).field(fieldName.toString(), (Object) null);
              }
            }
          }

          if (name != null) {
              assignVariable(iContext, name, target);
          }

        } else {
            assignVariable(iContext, name, v);
        }
      }
    }

    return null;
  }

  @Override
  public String getName() {
    return NAME;
  }
}
