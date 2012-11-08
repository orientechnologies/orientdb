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
package com.orientechnologies.orient.core.processor;

import com.orientechnologies.common.factory.ODynamicFactory;
import com.orientechnologies.orient.core.processor.block.OIteratorBlock;
import com.orientechnologies.orient.core.processor.block.OListBlock;
import com.orientechnologies.orient.core.processor.block.OProcessorBlock;
import com.orientechnologies.orient.core.processor.block.OQueryBlock;
import com.orientechnologies.orient.core.processor.block.OTableBlock;
import com.orientechnologies.orient.core.processor.block.OTextBlock;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OConfigurableProcessor extends ODynamicFactory<String, OProcessorBlock> implements OProcessor {

  public OConfigurableProcessor() {
    register(new OIteratorBlock());
    register(new OListBlock());
    register(new OQueryBlock());
    register(new OTableBlock());
    register(new OTextBlock());
  }

  public Object process(final Object iContent, final ODocument iContext, final boolean iReadOnly) {
    if (!(iContent instanceof ODocument))
      throw new OProcessException("Configurable processor needs a document");

    final ODocument document = (ODocument) iContent;

    final String type = document.field("type");
    if (type == null)
      throw new OProcessException("Configurable processor needs 'type' field");

    final Object res = process(type, document, iContext, iReadOnly);

    final ODocument result = new ODocument().field("result", res);
    if (res instanceof ODocument)
      ((ODocument) res).addOwner(result);
    return result;
  }

  public Object process(final String iType, final ODocument iContent, final ODocument iContext, final boolean iReadOnly) {
    if (iContent == null)
      throw new OProcessException("Cannot find block type '" + iType + "'");

    final OProcessorBlock block = registry.get(iType);
    if (block == null)
      throw new OProcessException("Cannot find block type '" + iType + "'");

    return block.process(this, (ODocument) iContent, iContext, iReadOnly);
  }

  public void register(final OProcessorBlock iValue) {
    super.register(iValue.getName(), iValue);
  }
}