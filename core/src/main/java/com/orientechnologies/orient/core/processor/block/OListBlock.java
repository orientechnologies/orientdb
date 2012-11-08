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

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.processor.OConfigurableProcessor;
import com.orientechnologies.orient.core.processor.OProcessException;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OListBlock extends OAbstractBlock {
  @Override
  public Object process(OConfigurableProcessor iManager, final ODocument iConfig, final ODocument iContext, final boolean iReadOnly) {
    final Object value = ((ODocument) iConfig).field("value");

    if (!OMultiValue.isIterable(value))
      throw new OProcessException("Content in not multi-value (collection, array, map)");

    final List<Object> list = new ArrayList<Object>();
    for (Object item : OMultiValue.getMultiValueIterable(value)) {
      final Object result;
      if (isBlock(item))
        result = iManager.process(null, (ODocument) item, iContext, iReadOnly);
      else
        result = resolveInContext(item, iContext);

      list.add(result);
    }

    return list;
  }

  @Override
  public String getName() {
    return "list";
  }
}