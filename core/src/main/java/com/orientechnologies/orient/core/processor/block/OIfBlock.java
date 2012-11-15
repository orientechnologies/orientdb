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
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OIfBlock extends OAbstractBlock {
  @Override
  public Object processBlock(OComposableProcessor iManager, final OCommandContext iContext, final ODocument iConfig,
      ODocument iOutput, final boolean iReadOnly) {
    Object ifClause = getRequiredFieldOfClass(iContext, iConfig, "if", String.class);
    Object thenClause = getRequiredFieldOfClass(iContext, iConfig, "then", String.class);
    Object elseClause = getFieldOfClass(iContext, iConfig, "else", String.class);

    Object ifResult = delegate("if", iManager, ifClause, iContext, iOutput, iReadOnly);

    final boolean execute;
    if (ifResult instanceof Boolean)
      execute = (Boolean) ifResult;
    else
      execute = Boolean.parseBoolean(thenClause.toString());

    if (execute)
      return delegate("then", iManager, thenClause, iContext, iOutput, iReadOnly);
    else if (elseClause != null)
      return delegate("else", iManager, elseClause, iContext, iOutput, iReadOnly);

    return null;
  }

  @Override
  public String getName() {
    return "if";
  }
}