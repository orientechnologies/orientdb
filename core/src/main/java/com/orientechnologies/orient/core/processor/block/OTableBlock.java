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

import com.orientechnologies.orient.core.processor.OConfigurableProcessor;
import com.orientechnologies.orient.core.processor.OProcessException;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OTableBlock extends OAbstractBlock {
  @Override
  public Object process(OConfigurableProcessor iManager, final ODocument iConfig, final ODocument iContext, final boolean iReadOnly) {
    if (!(iConfig instanceof ODocument))
      throw new OProcessException("Content in not a JSON");

    final ODocument content = (ODocument) iConfig;

    final ODocument table = new ODocument();

    table.field("header", delegate("header", iManager, content.field("header"), iContext, iReadOnly));
    table.field("body", delegate("body", iManager, content.field("body"), iContext, iReadOnly));
    table.field("footer", delegate("footer", iManager, content.field("footer"), iContext, iReadOnly));

    return table;
  }

  @Override
  public String getName() {
    return "table";
  }
}