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

package com.orientechnologies.orient.etl.transform;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilter;

public class OSQLFieldTransformer extends OAbstractTransformer {
  protected String     fieldName;
  protected String     expression;
  protected OSQLFilter sqlFilter;

  @Override
  public void configure(final ODocument iConfiguration) {
    if (iConfiguration.containsField("name"))
      fieldName = iConfiguration.field("name");
    if (iConfiguration.containsField("expression"))
      expression = iConfiguration.field("expression");
  }

  @Override
  public void prepare(final ODatabaseDocumentTx iDatabase) {
    super.prepare(iDatabase);
  }

  @Override
  public String getName() {
    return "field";
  }

  @Override
  public Object transform(final Object input, final OCommandContext iContext) {
    if (input == null)
      return null;

    if (input instanceof ODocument) {
      final Object value = ((ODocument) input).field(fieldName);
      if (value != null) {
        if (sqlFilter == null)
          // ONLY THE FIRST TIME
          sqlFilter = new OSQLFilter(expression, iContext, null);

        final Object newValue = sqlFilter.evaluate((ODocument) input, null, iContext);

        // SET THE TRANSFORMED FIELD BACK
        ((ODocument) input).field(fieldName, newValue);
      }
    }

    return input;
  }
}
