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
import java.util.List;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.processor.OComposableProcessor;
import com.orientechnologies.orient.core.processor.OProcessException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;

public class OListBlock extends OAbstractBlock {
  @SuppressWarnings("unchecked")
  @Override
  public Object processBlock(OComposableProcessor iManager, final ODocument iConfig, final OCommandContext iContext,
      final boolean iReadOnly) {

    final Object source = resolveInContext(getFieldOfClass(iConfig, "source", String.class), iContext);
    final Object values = resolveInContext(getRequiredField(iConfig, "values"), iContext);
    final String bind = getFieldOfClass(iConfig, "bind", String.class);
    final Boolean merge = getFieldOfClass(iConfig, "merge", Boolean.class);
    final Object onNull = getField(iConfig, "onNull");

    if (!OMultiValue.isIterable(values))
      throw new OProcessException("Field 'values' in not a multi-value (collection, array, map). Found type '" + values.getClass()
          + "'");

    final List<Object> list = new ArrayList<Object>();
    for (Object item : OMultiValue.getMultiValueIterable(values)) {
      final Object result;

      if (isBlock(item))
        result = iManager.process((ODocument) item, iContext, iReadOnly);
      else {
        if (source != null)
          item = ODocumentHelper.getFieldValue(source, item.toString());

        result = resolveInContext(item, iContext);
      }

      if (result == null) {
        if (onNull != null)
          list.add(onNull);
      } else if (merge != null && merge && result instanceof List<?>)
        list.addAll((Collection<? extends Object>) result);
      else
        list.add(result);
    }

    if (bind != null)
      iContext.setVariable(bind, list);

    return list;
  }

  @Override
  public String getName() {
    return "list";
  }
}