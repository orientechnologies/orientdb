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

package com.orientechnologies.orient.etl.block;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilter;
import com.orientechnologies.orient.etl.OETLProcessor;

public class OLetBlock extends OAbstractBlock {
  protected String     name;
  protected OSQLFilter expression;
  protected Object     value;

  @Override
  public ODocument getConfiguration() {
    return new ODocument().fromJSON("{parameters:[{name:{optional:false,description:'Variable name'}},"
        + "{value:{optional:true,description:'Variable value'}}"
        + "{expression:{optional:true,description:'Expression to evaluate'}}" + "]}");
  }

  @Override
  public void configure(OETLProcessor iProcessor, final ODocument iConfiguration, final OCommandContext iContext) {
    super.configure(iProcessor, iConfiguration, iContext);

    name = iConfiguration.field("name");
    if (iConfiguration.containsField("value")) {
      value = iConfiguration.field("value");
    } else
      expression = new OSQLFilter((String) iConfiguration.field("expression"), iContext, null);

    if (value == null && expression == null)
      throw new IllegalArgumentException("'value' or 'expression' parameter are mandatory in Let Transformer");
  }

  @Override
  public String getName() {
    return "let";
  }

  @Override
  public Object executeBlock() {
    final Object v = expression != null ? expression.evaluate(null, null, context) : resolve(value);
    context.setVariable(name, v);
    return v;
  }
}
