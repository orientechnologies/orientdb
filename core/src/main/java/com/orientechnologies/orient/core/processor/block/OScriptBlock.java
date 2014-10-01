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

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.processor.OComposableProcessor;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OScriptBlock extends OAbstractBlock {
  public static final String NAME = "script";

  @Override
  public Object processBlock(OComposableProcessor iManager, final OCommandContext iContext, final ODocument iConfig,
      ODocument iOutput, final boolean iReadOnly) {
    final String language = getFieldOrDefault(iContext, iConfig, "language", "javascript");

    Object code = getRequiredField(iContext, iConfig, "code");

    if (OMultiValue.isMultiValue(code)) {
      // CONCATS THE SNIPPET IN A BIG ONE
      final StringBuilder buffer = new StringBuilder(1024);
      for (Object o : OMultiValue.getMultiValueIterable(code)) {
        if (buffer.length() > 0)
          buffer.append(";");
        buffer.append(o.toString());
      }
      code = buffer.toString();
    }

    final OCommandScript script = new OCommandScript(language, code.toString());
    script.getContext().setParent(iContext);

    iContext.setVariable("block", this);

    return script.execute();
  }

  @Override
  public String getName() {
    return NAME;
  }
}
