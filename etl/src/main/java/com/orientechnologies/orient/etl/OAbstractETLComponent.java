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
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilter;
import com.orientechnologies.orient.core.sql.filter.OSQLPredicate;
import com.orientechnologies.orient.etl.OETLProcessor.LOG_LEVELS;

/**
 * ETL abstract component.
 */
public abstract class OAbstractETLComponent implements OETLComponent {
  protected OETLProcessor   processor;
  protected OCommandContext context;
  protected LOG_LEVELS      logLevel;
  protected String          output;
  protected String          ifExpression;

  @Override
  public ODocument getConfiguration() {
    return new ODocument().fromJSON("{parameters:[" + getCommonConfigurationParameters() + "]");
  }

  @Override
  public void configure(final OETLProcessor iProcessor, final ODocument iConfiguration, final OCommandContext iContext) {
    processor = iProcessor;
    context = iContext;

    ifExpression = iConfiguration.field("if");

    if (iConfiguration.containsField("log"))
      logLevel = LOG_LEVELS.valueOf(iConfiguration.field("log").toString().toUpperCase());
    else
      logLevel = iProcessor.getLogLevel();

    if (iConfiguration.containsField("output"))
      output = iConfiguration.field("output");
  }

  @Override
  public void begin() {
  }

  @Override
  public void end() {
  }

  protected String getCommonConfigurationParameters() {
    return "{log:{optional:true,description:'Can be any of [NONE, ERROR, INFO, DEBUG]. Default is INFO'}},"
           + "{if:{optional:true,description:'Conditional expression. If true, the block is executed, otherwise is skipped'}},"
           + "{output:{optional:true,description:'Variable name to store the transformer output. If null, the output will be passed to the pipeline as input for the next component.'}}";

  }

  @Override
  public String toString() {
    return getName();
  }

  protected boolean skip(final Object input) {
    final OSQLFilter ifFilter = getIfFilter();
    if (ifFilter != null) {
      final ODocument doc = input instanceof OIdentifiable ? (ODocument) ((OIdentifiable) input).getRecord() : null;

      log(LOG_LEVELS.DEBUG, "Evaluating conditional expression if=%s...", ifFilter);

      final Object result = ifFilter.evaluate(doc, null, context);
      if (!(result instanceof Boolean))
        throw new OConfigurationException("'if' expression in Transformer " + getName() + " returned '" + result
                                          + "' instead of boolean");

      return !(Boolean) result;
    }
    return false;
  }

  protected OSQLFilter getIfFilter() {
    if (ifExpression != null)
      return new OSQLFilter(ifExpression, context, null);
    return null;
  }

  protected void log(final LOG_LEVELS iLevel, String iText, final Object... iArgs) {
    if (logLevel.ordinal() >= iLevel.ordinal()) {
      final Long extractedNum = context != null ? (Long) context.getVariable("extractedNum") : null;
      if (extractedNum != null)
        System.out.println("[" + extractedNum + ":" + getName() + "] " + iLevel + " " + String.format(iText, iArgs));
      else
        System.out.println("[" + getName() + "] " + iLevel + " " + String.format(iText, iArgs));

    }
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

    Object value;
    if (iContent instanceof String) {
      if (((String) iContent).startsWith("$") && !((String) iContent).startsWith(OSystemVariableResolver.VAR_BEGIN))
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
}
