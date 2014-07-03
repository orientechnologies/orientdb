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

package com.orientechnologies.orient.etl.transformer;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilter;
import com.orientechnologies.orient.etl.OETLProcessor;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class OSkipTransformer extends OAbstractTransformer {
  protected String     expression;
  protected OSQLFilter sqlFilter;

  @Override
  public ODocument getConfiguration() {
    return new ODocument().fromJSON("{parameters:[{expression:{optional:false,description:'Expression to evaluate'}}],"
        + "input:['ODocument'],output:'ODocument'}");
  }

  @Override
  public void configure(OETLProcessor iProcessor, final ODocument iConfiguration, OBasicCommandContext iContext) {
    super.configure(iProcessor, iConfiguration, iContext);
    expression = iConfiguration.field("expression");
    sqlFilter = new OSQLFilter(expression, context, null);
  }

  @Override
  public String getName() {
    return "skip";
  }

  @Override
  public Object executeTransform(final Object input) {
    ODocument doc;
    if (input instanceof ODocument)
      doc = (ODocument) input;
    else if (input instanceof OrientVertex)
      doc = ((OrientVertex) input).getRecord();
    else
      throw new IllegalArgumentException(getName() + " transformer: unsupported input object '" + input + "' of class: "
          + input.getClass());

    final Boolean result = (Boolean) sqlFilter.evaluate(doc, null, context);
    if (result)
      // TRUE: SKIP IT
      return null;

    return input;
  }
}
