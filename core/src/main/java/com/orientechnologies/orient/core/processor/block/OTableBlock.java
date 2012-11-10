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
  public Object process(OComposableProcessor iManager, final ODocument iConfig, final OCommandContext iContext, final boolean iReadOnly) {
    if (!(iConfig instanceof ODocument))
      throw new OProcessException("Content in not a JSON");

    final ODocument header = getRequiredFieldOfClass(iConfig, "header", ODocument.class);
    final ODocument body = getRequiredFieldOfClass(iConfig, "body", ODocument.class);
    final ODocument footer = getRequiredFieldOfClass(iConfig, "footer", ODocument.class);

    final ODocument content = (ODocument) iConfig;

    final ODocument table = new ODocument();

    table.field("header", delegate("header", iManager, header, iContext, iReadOnly));
    table.field("body", delegate("body", iManager, body, iContext, iReadOnly));
    table.field("footer", delegate("footer", iManager, footer, iContext, iReadOnly));

    return table;
  }

  @Override
  public String getName() {
    return "table";
  }
}