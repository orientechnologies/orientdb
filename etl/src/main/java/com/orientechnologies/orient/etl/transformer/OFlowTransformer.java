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

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.OETLProcessHaltedException;
import com.orientechnologies.orient.etl.OETLProcessor;

public class OFlowTransformer extends OAbstractTransformer {
  private String operation;

  @Override
  public ODocument getConfiguration() {
    return new ODocument().fromJSON("{parameters:[" + getCommonConfigurationParameters() + ","
        + "{operation:{optional:false,description:'Flow operation between: skip and halt'}}],"
        + "input:['Object'],output:'Object'}");
  }

  @Override
  public void configure(OETLProcessor iProcessor, final ODocument iConfiguration, OCommandContext iContext) {
    super.configure(iProcessor, iConfiguration, iContext);
    operation = iConfiguration.field("operation");
    if (operation == null)
      throw new OConfigurationException("Flow transformer has not mandatory 'operation' field");
    if (!operation.equalsIgnoreCase("halt") && !operation.equalsIgnoreCase("skip"))
      throw new OConfigurationException("Flow transformer has invalid 'operation' field='" + operation
          + "', while supported are: 'skip' and 'halt'");
  }

  @Override
  public String getName() {
    return "flow";
  }

  @Override
  public Object executeTransform(final Object input) {
    if (operation.equalsIgnoreCase("skip"))
      return null;

    throw new OETLProcessHaltedException("Process stopped because this condition: " + ifExpression);
  }
}
