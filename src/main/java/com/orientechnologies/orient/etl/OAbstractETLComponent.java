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

import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.parser.OVariableParser;
import com.orientechnologies.common.parser.OVariableParserListener;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilter;
import com.orientechnologies.orient.core.sql.filter.OSQLPredicate;

/**
 * ETL abstract component.
 */
public abstract class OAbstractETLComponent implements OETLComponent {
  protected OETLProcessor            processor;
  protected OBasicCommandContext     context;
  protected OSQLFilter               ifFilter;
  protected OETLProcessor.LOG_LEVELS logLevel;
  protected String                   output = null;

  @Override
  public void configure(final OETLProcessor iProcessor, final ODocument iConfiguration, final OBasicCommandContext iContext) {
    processor = iProcessor;
    context = iContext;

    final String ifExpression = (String) resolve(iConfiguration.field("if"));
    if (ifExpression != null)
      ifFilter = new OSQLFilter(ifExpression, iContext, null);

    if (iConfiguration.containsField("log"))
      logLevel = OETLProcessor.LOG_LEVELS.valueOf((String) iConfiguration.field("log"));
    else
      logLevel = iProcessor.getLogLevel();

    if (iConfiguration.containsField("output"))
      output = (String) iConfiguration.field("output");
  }

  @Override
  public void begin() {
  }

  @Override
  public void end() {
  }

  @Override
  public ODocument getConfiguration() {
    return new ODocument().fromJSON("{parameters:[" + getCommonConfigurationParameters() + "]");
  }

  protected String getCommonConfigurationParameters() {
    return "{log:{optional:true,description:'Can be any of [NONE, ERROR, INFO, DEBUG]. Default is INFO'}},"
        + "{if:{optional:true,description:'Conditional expression. If true, the block is executed, otherwise is skipped'}},"
        + "{output:{optional:true,description:'Variable name to store the transformer output. If null, the output will be passed to the pipeline as input for the next component.'}}";

  }

  protected String stringArray2Json(final Object[] iObject) {
    final StringBuilder buffer = new StringBuilder(256);
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

  protected Object resolve(final Object iContent) {
    if (context == null || iContent == null)
      return iContent;

    Object value = null;
    if (iContent instanceof String) {
      if (((String) iContent).startsWith("$"))
        value = context.getVariable(iContent.toString());
      else
        value = OVariableParser.resolveVariables((String) iContent, OSystemVariableResolver.VAR_BEGIN,
            OSystemVariableResolver.VAR_END, new OVariableParserListener() {
              @Override
              public Object resolve(final String iVariable) {
                return context.getVariable(iVariable);
              }
            });
    } else
      value = iContent;

    if (value instanceof String)
      value = OVariableParser.resolveVariables((String) value, "={", "}", new OVariableParserListener() {

        @Override
        public Object resolve(final String iVariable) {
          return new OSQLPredicate(iVariable).evaluate(context);
        }

      });
    return value;
  }

  protected void log(final OETLProcessor.LOG_LEVELS iLevel, final String iText, final Object... iArgs) {
    if (logLevel.ordinal() >= iLevel.ordinal()) {
      Long extractedNum = (Long) context.getVariable("extractedNum");
      if (extractedNum != null)
        System.out.println("[" + extractedNum + ":" + getName() + "] " + iLevel + " " + String.format(iText, iArgs));
      else
        System.out.println("[" + getName() + "] " + iLevel + " " + String.format(iText, iArgs));
    }
  }
}
