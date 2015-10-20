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
import com.orientechnologies.orient.etl.OETLPipeline;
import com.orientechnologies.orient.etl.OETLProcessor;
import com.orientechnologies.orient.etl.block.OBlock;

/**
 * Pass-through Transformer that execute a block.
 */
public class OBlockTransformer extends OAbstractTransformer {
  private OBlock block;

  @Override
  public ODocument getConfiguration() {
    return new ODocument().fromJSON("{parameters:[" + getCommonConfigurationParameters() + ","
        + "{block:{optional:false,description:'Block to execute'}}]}");
  }

  @Override
  public void configure(OETLProcessor iProcessor, final ODocument iConfiguration, OCommandContext iContext) {
    super.configure(iProcessor, iConfiguration, iContext);
    final String[] fieldNames = iConfiguration.fieldNames();

    try {
      block = processor.getFactory().getBlock(fieldNames[0]);
      block.configure(processor, (ODocument) iConfiguration.field(fieldNames[0]), context);
    } catch (Exception e) {
      throw new OConfigurationException("[Block transformer] Error on configuring inner block", e);
    }
  }

  @Override
  public String getName() {
    return "block";
  }

  @Override
  public void setPipeline(OETLPipeline iPipeline) {
    super.setPipeline(iPipeline);
    block.setContext(context);
  }

  @Override
  protected Object executeTransform(final Object input) {
    context.setVariable("input", input);
    block.execute();
    return input;
  }
}
