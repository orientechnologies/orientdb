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
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.processor.OConfigurableProcessor;
import com.orientechnologies.orient.core.processor.OProcessException;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OIteratorBlock extends OAbstractBlock {
  @Override
  public Object process(OConfigurableProcessor iManager, final ODocument iConfig, final ODocument iContext, final boolean iReadOnly) {
    if (!(iConfig instanceof ODocument))
      throw new OProcessException("Content in not a JSON");

    final ODocument content = (ODocument) iConfig;

    final Object foreach = content.field("foreach");
    if (!(foreach instanceof ODocument))
      throw new OProcessException("'foreach' must be a block");

    final Object execute = content.field("execute");
    if (!(execute instanceof ODocument))
      throw new OProcessException("'execute' must be a block");

    final Object result = iManager.process((ODocument) foreach, iContext, iReadOnly);
    if (!OMultiValue.isIterable(result))
      throw new OProcessException("Result of 'foreach' block (" + foreach + ") must be iterable but found " + result.getClass());

    final String executeBlockType = ((ODocument) execute).field("type");

    final List<Object> list = new ArrayList<Object>();
    for (Object item : OMultiValue.getMultiValueIterable(result)) {
      if (item instanceof Map.Entry)
        item = ((Entry<?, ?>) item).getValue();

      iContext.field("content", item);

      final Object v = iManager.process(executeBlockType, (ODocument) execute, iContext, iReadOnly);
      if (v != null)
        list.add(v);
    }

    return list;
  }

  @Override
  public String getName() {
    return "iterator";
  }
}