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
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilter;
import com.orientechnologies.orient.etl.OETLProcessor;

import java.util.List;

public class OFieldTransformer extends OAbstractTransformer {
  private String       fieldName;
  private List<String> fieldNames;
  private String       expression;
  private Object       value;
  private boolean      setOperation = true;
  private OSQLFilter   sqlFilter;
  private boolean      save         = false;

  @Override
  public ODocument getConfiguration() {
    return new ODocument().fromJSON("{parameters:[" + getCommonConfigurationParameters() + ","
        + "{fieldName:{optional:true,description:'field name to apply the result'}},"
        + "{fieldNames:{optional:true,description:'field names to apply the result'}},"
        + "{expression:{optional:true,description:'expression to evaluate. Mandatory with operation=set (default)'}}"
        + "{value:{optional:true,description:'value to set'}}"
        + "{operation:{optional:false,description:'operation to execute against the field: set, remove. Default is set'}}"
        + "{save:{optional:true,description:'save the vertex/edge/document right after the setting of the field'}}" + "],"
        + "input:['ODocument'],output:'ODocument'}");
  }

  @Override
  public void configure(OETLProcessor iProcessor, final ODocument iConfiguration, OCommandContext iContext) {
    super.configure(iProcessor, iConfiguration, iContext);
    fieldName = (String) resolve(iConfiguration.field("fieldName"));
    fieldNames = (List<String>) resolve(iConfiguration.field("fieldNames"));

    if (fieldNames == null && fieldName == null)
      throw new IllegalArgumentException("Field transformer must specify 'fieldName' or 'fieldNames'");

    expression = iConfiguration.field("expression");
    value = iConfiguration.field("value");

    if (expression != null && value != null)
      throw new IllegalArgumentException("Field transformer cannot specify both 'expression' and 'value'");

    if (iConfiguration.containsField("save"))
      save = (Boolean) iConfiguration.field("save");

    if (iConfiguration.containsField("operation"))
      setOperation = "set".equalsIgnoreCase((String) iConfiguration.field("operation"));
  }

  @Override
  public String getName() {
    return "field";
  }

  @Override
  public Object executeTransform(final Object input) {
    if (input instanceof OIdentifiable) {
      final ORecord rec = ((OIdentifiable) input).getRecord();

      if (rec instanceof ODocument) {
        final ODocument doc = (ODocument) rec;

        if (setOperation) {
          final Object newValue;
          if (expression != null) {
            if (sqlFilter == null)
              // ONLY THE FIRST TIME
              sqlFilter = new OSQLFilter(expression, context, null);

            newValue = sqlFilter.evaluate(doc, null, context);
          } else
            newValue = value;

          // SET THE TRANSFORMED FIELD BACK
          doc.field(fieldName, newValue);

          log(OETLProcessor.LOG_LEVELS.DEBUG, "set %s=%s in document=%s", fieldName, newValue, doc);
        } else {
          if (fieldName != null) {
            final Object prev = doc.removeField(fieldName);
            log(OETLProcessor.LOG_LEVELS.DEBUG, "removed %s (value=%s) from document=%s", fieldName, prev, doc);
          } else {
            for (String f : fieldNames) {
              final Object prev = doc.removeField(f);
              log(OETLProcessor.LOG_LEVELS.DEBUG, "removed %s (value=%s) from document=%s", f, prev, doc);
            }
          }
        }

        if (save) {
          log(OETLProcessor.LOG_LEVELS.DEBUG, "saving record %s", doc);
          final ODatabaseDocumentTx db = super.pipeline.getDocumentDatabase();
          db.save(doc);
        }
      }
    }

    return input;
  }
}
