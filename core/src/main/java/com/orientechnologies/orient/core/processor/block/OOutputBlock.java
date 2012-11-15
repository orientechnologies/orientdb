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
import java.util.List;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.processor.OComposableProcessor;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OOutputBlock extends OAbstractBlock {
  @SuppressWarnings("unchecked")
  @Override
  public Object processBlock(OComposableProcessor iManager, final OCommandContext iContext, final ODocument iConfig,
      ODocument iOutput, final boolean iReadOnly) {

    final Object value = getRequiredField(iContext, iConfig, "value");

    Object result;
    if (isBlock(value))
      result = delegate("value", iManager, value, iContext, iOutput, iReadOnly);
    else
      result = value;

    final Object source = getField(iContext, iConfig, "source");
    if (source instanceof ODocument && result instanceof List<?>) {
      final List<Object> list = new ArrayList<Object>();
      for (Object o : (List<Object>) result) {
        if (o != null)
          list.add(((ODocument) source).field(o.toString()));
      }
      result = list;
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

  @Override
  public String getName() {
    return "output";
  }
}