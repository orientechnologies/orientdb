/*
 *
 *  * Copyright 2010-2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.orient.etl;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilter;

/**
 * ETL abstract component.
 */
public abstract class OAbstractETLComponent implements OETLComponent {
  protected OETLProcessor        processor;
  protected OBasicCommandContext context;
  protected OSQLFilter           ifFilter;

  @Override
  public void configure(final OETLProcessor iProcessor, final ODocument iConfiguration, final OBasicCommandContext iContext) {
    processor = iProcessor;
    context = iContext;

    final String ifExpression = iConfiguration.field("if");
    if (ifExpression != null)
      ifFilter = new OSQLFilter(ifExpression, iContext, null);
  }

  @Override
  public void begin() {
  }

  @Override
  public void end() {
  }

  protected String stringArray2Json(final Object[] iObject) {
    final StringBuilder buffer = new StringBuilder();
    buffer.append('[');
    for (int i = 0; i < iObject.length; ++i) {
      if (i > 0)
        buffer.append(',');

      final Object value = iObject[i];
      if (value != null) {
        buffer.append("'");
        buffer.append(value.toString());
        buffer.append("'");
      }
    }
    buffer.append(']');
    return buffer.toString();
  }

  protected <T> T resolveVariable(final String iName) {
    if (iName != null && iName.startsWith("$"))
      return (T) context.getVariable(iName);
    return (T) iName;
  }
}
