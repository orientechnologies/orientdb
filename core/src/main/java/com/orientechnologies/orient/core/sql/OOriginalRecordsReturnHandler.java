/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.record.impl.ODocument;

/** @author Artem Orobets (enisher-at-gmail.com) */
public class OOriginalRecordsReturnHandler extends ORecordsReturnHandler {
  public OOriginalRecordsReturnHandler(Object returnExpression, OCommandContext context) {
    super(returnExpression, context);
  }

  @Override
  protected ODocument preprocess(ODocument result) {
    return result.copy();
  }

  @Override
  public void beforeUpdate(ODocument result) {
    storeResult(result);
  }

  @Override
  public void afterUpdate(ODocument result) {}
}
