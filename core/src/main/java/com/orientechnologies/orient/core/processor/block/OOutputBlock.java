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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.processor.OComposableProcessor;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OOutputBlock extends OAbstractBlock {
  public static final String NAME = "output";

  @SuppressWarnings("unchecked")
  @Override
  public Object processBlock(OComposableProcessor iManager, final OCommandContext iContext, final ODocument iConfig,
      ODocument iOutput, final boolean iReadOnly) {

    final Object value = getRequiredField(iContext, iConfig, "value");
    Boolean nullAsEmpty = getFieldOfClass(iContext, iConfig, "nullAsEmpty", Boolean.class);
    if (nullAsEmpty == null)
      nullAsEmpty = true;
    final Boolean flatMultivalues = getFieldOfClass(iContext, iConfig, "flatMultivalues", Boolean.class);

    Object result;
    if (isBlock(value))
      result = delegate("value", iManager, value, iContext, iOutput, iReadOnly);
    else
      result = value;

    Object source = getField(iContext, iConfig, "source");

    if (source instanceof Map<?, ?>)
      source = new ODocument((Map<String, Object>) source);

    if (source instanceof ODocument && result instanceof List<?>) {
      result = addDocumentFields((ODocument) source, (List<Object>) result, nullAsEmpty);
    } else if (OMultiValue.isMultiValue(result) && flatMultivalues != null && flatMultivalues) {
      result = flatMultivalues(iContext, false, flatMultivalues, result);
    }

    final String field = getFieldOfClass(iContext, iConfig, "field", String.class);
    if (field != null) {
      // WRITE TO THE OUTPUT
      iOutput.field(field, result);
      return iOutput;
    }

    // NO FIELD: RETURN THE VALUE
    return result;
  }

  public static Object addDocumentFields(final ODocument source, List<Object> result) {
    return addDocumentFields(source, result, true);
  }

  public static Object addDocumentFields(final ODocument source, List<Object> result, boolean nullAsEmpty) {
    final List<Object> list = new ArrayList<Object>();
    for (Object o : result) {
      if (o != null) {
        final Object fieldValue = source.field(o.toString());
        if (fieldValue == null && nullAsEmpty)
          list.add("");
        else
          list.add(fieldValue);
      }
    }
    return list;
  }

  @Override
  public String getName() {
    return NAME;
  }
}
