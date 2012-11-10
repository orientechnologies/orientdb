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
  public Object process(OComposableProcessor iManager, final ODocument iConfig, final OCommandContext iContext,
      final boolean iReadOnly) {
    final Boolean copy = getFieldOfClass(iConfig, "copy", Boolean.class);

    Object values = getField(iConfig, "values");
    if (values != null) {
      if (values instanceof ODocument) {
        final ODocument doc = ((ODocument) values);
        for (String fieldName : doc.fieldNames())
          iContext.setVariable(resolveInContext(fieldName, iContext).toString(), resolveInContext(doc.field(fieldName), iContext));
      } else if (values instanceof Map<?, ?>) {
        for (Entry<Object, Object> entry : ((Map<Object, Object>) values).entrySet())
          iContext.setVariable(resolveInContext(entry.getKey(), iContext).toString(),
              getValue(resolveInContext(entry.getValue(), iContext), copy));

      } else
        throw new OProcessException("Field 'values' in not a multi-value (collection, array, map). Found type '"
            + values.getClass() + "'");

    } else {
      final String name = getRequiredFieldOfClass(iConfig, "name", String.class);

      Object value = getRequiredField(iConfig, "value");
      if (value instanceof String)
        value = resolveInContext(value, iContext);
      assignVariable(iContext, name, getValue(value, copy));
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