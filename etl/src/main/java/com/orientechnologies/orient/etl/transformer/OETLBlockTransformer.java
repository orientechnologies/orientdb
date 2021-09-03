/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (info(-at-)orientdb.com)
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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.OETLComponentFactory;
import com.orientechnologies.orient.etl.block.OETLBlock;

/** Pass-through Transformer that execute a block. */
public class OETLBlockTransformer extends OETLAbstractTransformer {
  private OETLBlock block;

  @Override
  public ODocument getConfiguration() {
    return new ODocument()
        .fromJSON(
            "{parameters:["
                + getCommonConfigurationParameters()
                + ","
                + "{block:{optional:false,description:'Block to execute'}}]}");
  }

  @Override
  public void configure(final ODocument iConfiguration, OCommandContext iContext) {
    super.configure(iConfiguration, iContext);
  }

  @Override
  public void begin(ODatabaseDocument db) {
    super.begin(db);

    final String[] fieldNames = configuration.fieldNames();

    try {
      String fieldName = fieldNames[0];
      block = new OETLComponentFactory().getBlock(fieldName);
      block.configure(configuration.<ODocument>field(fieldName), context);
    } catch (Exception e) {
      throw OException.wrapException(
          new OConfigurationException("[Block transformer] Error on configuring inner block"), e);
    }
  }

  @Override
  public String getName() {
    return "block";
  }

  @Override
  public void setContext(OCommandContext context) {
    block.setContext(context);
  }

  @Override
  protected Object executeTransform(ODatabaseDocument db, final Object input) {
    context.setVariable("input", input);
    block.execute();
    return input;
  }
}
