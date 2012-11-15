/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.processor.block;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.processor.OComposableProcessor;
import com.orientechnologies.orient.core.processor.OProcessException;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OLetBlock extends OAbstractBlock {
  @SuppressWarnings("unchecked")
  @Override
  public Object processBlock(OComposableProcessor iManager, final OCommandContext iContext, final ODocument iConfig,
      ODocument iOutput, final boolean iReadOnly) {
    final Boolean copy = getFieldOfClass(iContext, iConfig, "copy", Boolean.class);
    final ODocument target = getFieldOfClass(iContext, iConfig, "target", ODocument.class);

    final Object value = getRawField(iConfig, "value");
    if (value != null) {
      if (value instanceof ODocument) {
        final ODocument doc = ((ODocument) value);
        for (String fieldName : doc.fieldNames()) {
          final Object v = resolveValue(iContext, doc.field(fieldName));
          if (target != null) {
            debug(iContext, "Set value %s in document field '%s'", v, fieldName);
            target.field(fieldName, v);
          } else
            assignVariable(iContext, fieldName, v);
        }
      } else if (value instanceof Map<?, ?>) {
        for (Entry<Object, Object> entry : ((Map<Object, Object>) value).entrySet()) {
          final Object v = resolveValue(iContext, getValue(entry.getValue(), copy));
          if (target != null) {
            debug(iContext, "Set value %s in document field '%s'", v, entry.getKey());
            target.field(entry.getKey().toString(), v);
          } else
            assignVariable(iContext, entry.getKey().toString(), v);
        }

      } else {
        final String name = getRequiredFieldOfClass(iContext, iConfig, "name", String.class);
        assignVariable(iContext, name, getValue(getRequiredField(iContext, iConfig, "value"), copy));
      }
    }

    return null;
  }

  @SuppressWarnings("unchecked")
  private Object getValue(final Object iValue, final Boolean iCopy) {
    if (iValue != null && iCopy != null && iCopy) {
      // COPY THE VALUE
      if (iValue instanceof ODocument)
        return ((ODocument) iValue).copy();
      else if (iValue instanceof List)
        return new ArrayList<Object>((Collection<Object>) iValue);
      else if (iValue instanceof Set)
        return new HashSet<Object>((Collection<Object>) iValue);
      else if (iValue instanceof Map)
        return new LinkedHashMap<Object, Object>((Map<Object, Object>) iValue);
      else
        throw new OProcessException("Copy of value '" + iValue + "' of class '" + iValue.getClass() + "' is not supported");
    }
    return iValue;
  }

  @Override
  public String getName() {
    return "let";
  }
}