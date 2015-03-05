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
package com.orientechnologies.orient.core.processor;

import com.orientechnologies.common.factory.OConfigurableStatefulFactory;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.processor.block.*;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class OComposableProcessor extends OConfigurableStatefulFactory<String, OProcessorBlock> implements OProcessor {

  private String path;
  private String extension;

  public OComposableProcessor() {
    register(OFunctionBlock.NAME, OFunctionBlock.class);
    register(OIfBlock.NAME, OIfBlock.class);
    register(OIterateBlock.NAME, OIterateBlock.class);
    register(OLetBlock.NAME, OLetBlock.class);
    register(OExecuteBlock.NAME, OExecuteBlock.class);
    register(OOutputBlock.NAME, OOutputBlock.class);
    register(OQueryBlock.NAME, OQueryBlock.class);
    register(OScriptBlock.NAME, OScriptBlock.class);
    register(OTableBlock.NAME, OTableBlock.class);
  }

  public Object processFromFile(final String iFileName, final OCommandContext iContext, final boolean iReadOnly) throws IOException {
    final ODocument template = new ODocument().fromJSON(loadTemplate(iFileName), "noMap");

    return process(null, template, iContext, new ODocument().setOrdered(true), iReadOnly);
  }

  public Object process(final OProcessorBlock iParent, final Object iContent, final OCommandContext iContext,
      final ODocument iOutput, final boolean iReadOnly) {
    if (!(iContent instanceof ODocument)) {
        throw new OProcessException("Composable processor needs a document");
    }

    final ODocument document = (ODocument) iContent;

    final String type = document.field("type");
    if (type == null) {
        throw new OProcessException("Composable processor needs 'type' field");
    }

    return process(iParent, type, document, iContext, iOutput, iReadOnly);
  }

  public Object process(final OProcessorBlock iParent, final String iType, final ODocument iContent,
      final OCommandContext iContext, final ODocument iOutput, final boolean iReadOnly) {
    if (iContent == null) {
        throw new OProcessException("Cannot find block type '" + iType + "'");
    }

    OProcessorBlock block;
    try {
      block = newInstance(iType);
    } catch (Exception e) {
      throw new OProcessException("Cannot create block of class '" + iType + "'", e);
    }

    block.setParentBlock(iParent);

    final Integer depthLevel = (Integer) iContext.getVariable("depthLevel");
    iContext.setVariable("depthLevel", depthLevel == null ? 0 : depthLevel + 1);

    if (depthLevel == null) {
        OLogManager.instance().info(this, "Start processing...");
    }

    final long start = System.currentTimeMillis();
    try {
      return block.process(this, iContext, (ODocument) iContent, iOutput, iReadOnly);
    } finally {
      iContext.setVariable("depthLevel", depthLevel == null ? 0 : depthLevel);

      if (depthLevel == null) {
          OLogManager.instance().info(this, "End of processing. Elapsed %dms", (System.currentTimeMillis() - start));
      }
    }
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
      for (int i = 0; i < contentSize; ++i) {
          buffer[i] = (byte) is.read();
      }

      return new String(buffer);

    } finally {
      is.close();
    }
  }
}
