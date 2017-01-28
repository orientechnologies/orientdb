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

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.logging.Level;

/**
 * ETL Transformer that logs the input.
 */
public class OETLLogTransformer extends OETLAbstractTransformer {
  private String prefix  = "";
  private String postfix = "";

  @Override
  public void configure(final ODocument iConfiguration, OCommandContext iContext) {
    super.configure(iConfiguration, iContext);
    if (iConfiguration.containsField("prefix"))
      prefix = iConfiguration.field("prefix");
    if (iConfiguration.containsField("postfix"))
      postfix = iConfiguration.field("postfix");
  }

  @Override
  public ODocument getConfiguration() {
    return new ODocument().fromJSON("{parameters:[" + getCommonConfigurationParameters() + ","
        + "{prefix:{optional:true,description:'Custom prefix to prepend to the message'}},"
        + "{postfix:{optional:true,description:'Custom postfix to append to the message'}}" + "]}");
  }

  @Override
  public String getName() {
    return "log";
  }

  @Override
  public Object executeTransform(ODatabaseDocument db, final Object input) {
    final StringBuilder buffer = new StringBuilder();

    if (prefix != null && !prefix.isEmpty())
      buffer.append(resolve(prefix));

    if (input != null)
      buffer.append(input);

    if (postfix != null && !postfix.isEmpty())
      buffer.append(resolve(postfix));

    log(Level.INFO, buffer.toString());

    return input;
  }
}
