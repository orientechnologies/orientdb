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

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.processor.OComposableProcessor;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OScriptBlock extends OAbstractBlock {
  @Override
  public Object processBlock(OComposableProcessor iManager, final ODocument iConfig, final OCommandContext iContext,
      final boolean iReadOnly) {
    final String language = getFieldOrDefault(iConfig, "language", "javascript");

    final Object code = getRequiredField(iConfig, "code");

    Object result = executeCodeSnippet(iContext, language, code);

    final String ret = getField(iConfig, "return");
    if (ret != null)
      iContext.setVariable(ret, result);

    return null;
  }

  private Object executeCodeSnippet(final OCommandContext iContext, final String iLanguage, Object iCode) {
    if (OMultiValue.isMultiValue(iCode)) {
      final StringBuilder buffer = new StringBuilder();
      for (Object o : OMultiValue.getMultiValueIterable(iCode)) {
        if (buffer.length() > 0)
          buffer.append(";");
        buffer.append(o.toString());
      }
      iCode = buffer.toString();
    }

    final OCommandScript script = new OCommandScript(iLanguage, iCode.toString());
    script.getContext().setParent(iContext);
    return script.execute();
  }

  @Override
  public String getName() {
    return "script";
  }
}