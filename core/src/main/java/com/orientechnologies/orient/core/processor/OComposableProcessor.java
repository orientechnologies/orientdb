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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import com.orientechnologies.common.factory.ODynamicFactory;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.processor.block.OExecuteBlock;
import com.orientechnologies.orient.core.processor.block.OFunctionBlock;
import com.orientechnologies.orient.core.processor.block.OIfBlock;
import com.orientechnologies.orient.core.processor.block.OIterateBlock;
import com.orientechnologies.orient.core.processor.block.OLetBlock;
import com.orientechnologies.orient.core.processor.block.OOutputBlock;
import com.orientechnologies.orient.core.processor.block.OProcessorBlock;
import com.orientechnologies.orient.core.processor.block.OQueryBlock;
import com.orientechnologies.orient.core.processor.block.OScriptBlock;
import com.orientechnologies.orient.core.processor.block.OTableBlock;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OComposableProcessor extends ODynamicFactory<String, OProcessorBlock> implements OProcessor {

  private String path;
  private String extension;

  public OComposableProcessor() {
    register(new OFunctionBlock());
    register(new OIfBlock());
    register(new OIterateBlock());
    register(new OLetBlock());
    register(new OExecuteBlock());
    register(new OOutputBlock());
    register(new OQueryBlock());
    register(new OScriptBlock());
    register(new OTableBlock());
  }

  public Object processFromFile(final String iFileName, final OCommandContext iContext, final boolean iReadOnly) throws IOException {
    final ODocument template = new ODocument().fromJSON(loadTemplate(iFileName), "noMap");

    return process(template, iContext, new ODocument().setOrdered(true), iReadOnly);
  }

  public Object process(final Object iContent, final OCommandContext iContext, final ODocument iOutput, final boolean iReadOnly) {
    if (!(iContent instanceof ODocument))
      throw new OProcessException("Composable processor needs a document");

    final ODocument document = (ODocument) iContent;

    final String type = document.field("type");
    if (type == null)
      throw new OProcessException("Composable processor needs 'type' field");

    return process(type, document, iContext, iOutput, iReadOnly);
  }

  public Object process(final String iType, final ODocument iContent, final OCommandContext iContext, final ODocument iOutput,
      final boolean iReadOnly) {
    if (iContent == null)
      throw new OProcessException("Cannot find block type '" + iType + "'");

    final OProcessorBlock block = registry.get(iType);
    if (block == null)
      throw new OProcessException("Cannot find block type '" + iType + "'");

    final Integer depthLevel = (Integer) iContext.getVariable("depthLevel");
    iContext.setVariable("depthLevel", depthLevel == null ? 0 : depthLevel + 1);

    if (depthLevel == null)
      OLogManager.instance().info(this, "Start processing...");

    final long start = System.currentTimeMillis();
    try {
      return block.process(this, iContext, (ODocument) iContent, iOutput, iReadOnly);
    } finally {
      iContext.setVariable("depthLevel", depthLevel == null ? 0 : depthLevel);

      if (depthLevel == null)
        OLogManager.instance().info(this, "End of processing. Elapsed %dms", (System.currentTimeMillis() - start));
    }
  }

  public void register(final OProcessorBlock iValue) {
    super.register(iValue.getName(), iValue);
  }

  public String getPath() {
    return path;
  }

  public OComposableProcessor setPath(String path) {
    this.path = path;
    return this;
  }

  public String getExtension() {
    return extension;
  }

  public OComposableProcessor setExtension(String extension) {
    this.extension = extension;
    return this;
  }

  protected String loadTemplate(final String iPath) throws IOException {
    final File file = new File(path + "/" + iPath + extension);
    final BufferedInputStream is = new BufferedInputStream(new FileInputStream(file));

    try {
      final long contentSize = file.length();

      // READ THE ENTIRE STREAM AND CACHE IT IN MEMORY
      final byte[] buffer = new byte[(int) contentSize];
      for (int i = 0; i < contentSize; ++i)
        buffer[i] = (byte) is.read();

      return new String(buffer);

    } finally {
      is.close();
    }
  }
}