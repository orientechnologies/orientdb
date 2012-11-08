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
import com.orientechnologies.orient.core.processor.block.OAbstractBlock;
import com.orientechnologies.orient.core.processor.block.OIteratorBlock;
import com.orientechnologies.orient.core.processor.block.OListBlock;
import com.orientechnologies.orient.core.processor.block.OProcessorBlock;
import com.orientechnologies.orient.core.processor.block.OQueryBlock;
import com.orientechnologies.orient.core.processor.block.OTableBlock;
import com.orientechnologies.orient.core.processor.block.OTextBlock;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OConfigurableProcessor extends ODynamicFactory<String, OProcessorBlock> implements OProcessor {

  public OConfigurableProcessor() {
    register("iterator", new OIteratorBlock());
    register("list", new OListBlock());
    register("query", new OQueryBlock());
    register("table", new OTableBlock());
    register("text", new OTextBlock());
  }

  public ODocument process(final Object iContent, final ODocument iContext, final boolean iReadOnly) {
    OAbstractBlock.checkForBlock(iContent);

    final ODocument document = (ODocument) iContent;

    final String type = document.field("type");
    if (type == null)
      throw new OProcessException("Configurable processor needs 'type' field");

    final Object content = document.field("content");
    if (content == null)
      throw new OProcessException("Configurable processor needs 'content' field");

    final Object result = process(type, content, iContext, iReadOnly);
    if (result instanceof ODocument)
      return (ODocument) result;

    return new ODocument().field("result", result);
  }

  public Object process(final String iType, final Object iContent, final ODocument iContext, final boolean iReadOnly) {
    final OProcessorBlock block = registry.get(iType);
    if (block == null)
      throw new OProcessException("Cannot find block type '" + iType + "'");

    return block.process(this, iContent, iContext, iReadOnly);
  }
}