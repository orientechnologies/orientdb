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

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.processor.OComposableProcessor;
import com.orientechnologies.orient.core.processor.OProcessException;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OTableBlock extends OAbstractBlock {
  @Override
  public Object processBlock(OComposableProcessor iManager, final OCommandContext iContext, final ODocument iConfig,
      ODocument iOutput, final boolean iReadOnly) {
    if (!(iConfig instanceof ODocument))
      throw new OProcessException("Content in not a JSON");

    final Object header = getRequiredField(iContext, iConfig, "header");
    final Object body = getRequiredField(iContext, iConfig, "body");
    final Object footer = getRequiredField(iContext, iConfig, "footer");

    final ODocument table = new ODocument();

    table.field("header", isBlock(header) ? delegate("header", iManager, header, iContext, iOutput, iReadOnly) : header);
    table.field("body", isBlock(body) ? delegate("body", iManager, body, iContext, iOutput, iReadOnly) : body);
    table.field("footer", isBlock(footer) ? delegate("footer", iManager, footer, iContext, iOutput, iReadOnly) : footer);

    return table;
  }

  @Override
  public String getName() {
    return "table";
  }
}