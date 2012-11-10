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
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;

public class OTextBlock extends OAbstractBlock {
  @Override
  public Object process(OComposableProcessor iManager, final ODocument iConfig, final OCommandContext iContext,
      final boolean iReadOnly) {

    Object value = resolveInContext(getRequiredField(iConfig, "value"), iContext);

    final Object source = resolveInContext(getFieldOfClass(iConfig, "source", String.class), iContext);
    if (source != null)
      value = ODocumentHelper.getFieldValue(source, value.toString());

    return resolveInContext(value, iContext).toString();
  }

  @Override
  public String getName() {
    return "text";
  }
}