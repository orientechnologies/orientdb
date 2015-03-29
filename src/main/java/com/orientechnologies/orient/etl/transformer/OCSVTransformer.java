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
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.etl.OETLProcessor;

import java.util.ArrayList;
import java.util.List;

public class OCSVTransformer extends OAbstractTransformer {
  private char         separator          = ',';
  private boolean      columnsOnFirstLine = true;
  private List<String> columnNames        = null;
  private List<OType>  columnTypes        = null;
  private long         skipFrom           = -1;
  private long         skipTo             = -1;
  private long         line               = -1;
  private String       nullValue;
  private char         stringCharacter    = '"';

  @Override
  public ODocument getConfiguration() {
    return new ODocument().fromJSON("{parameters:[" + getCommonConfigurationParameters()
        + ",{separator:{optional:true,description:'Column separator'}},"
        + "{columnsOnFirstLine:{optional:true,description:'Columns are described in the first line'}},"
        + "{columns:{optional:true,description:'Columns array containing names, and optionally type after :'}},"
        + "{nullValue:{optional:true,description:'value to consider as NULL. Default is not declared'}},"
        + "{stringCharacter:{optional:true,description:'String character delimiter'}},"
        + "{skipFrom:{optional:true,description:'Line number where start to skip',type:'int'}},"
        + "{skipTo:{optional:true,description:'Line number where skip ends',type:'int'}}"
        + "],input:['String'],output:'ODocument'}");
  }

  @Override
  public void configure(final OETLProcessor iProcessor, final ODocument iConfiguration, final OBasicCommandContext iContext) {
    super.configure(iProcessor, iConfiguration, iContext);

    if (iConfiguration.containsField("separator"))
      separator = iConfiguration.field("separator").toString().charAt(0);
    if (iConfiguration.containsField("columnsOnFirstLine"))
      columnsOnFirstLine = (Boolean) iConfiguration.field("columnsOnFirstLine");
    if (iConfiguration.containsField("columns")) {
      final List<String> columns = iConfiguration.field("columns");
      columnNames = new ArrayList<String>(columns.size());
      columnTypes = new ArrayList<OType>(columns.size());
      for (String c : columns) {
        final String[] parts = c.split(":");

        columnNames.add(parts[0]);
        if (parts.length > 1)
          columnTypes.add(OType.valueOf(parts[1].toUpperCase()));
        else
          columnTypes.add(OType.ANY);
      }
    }
    if (iConfiguration.containsField("skipFrom"))
      skipFrom = ((Number) iConfiguration.field("skipFrom")).longValue();
    if (iConfiguration.containsField("skipTo"))
      skipTo = ((Number) iConfiguration.field("skipTo")).longValue();
    if (iConfiguration.containsField("nullValue"))
      nullValue = iConfiguration.field("nullValue");
    if (iConfiguration.containsField("stringCharacter"))
      stringCharacter = iConfiguration.field("stringCharacter").toString().charAt(0);
  }

  @Override
  public String getName() {
    return "csv";
  }

  @Override
  public Object executeTransform(final Object input) {
    line++;

    if (skipFrom > -1) {
      if (skipTo > -1) {
        if (line >= skipFrom && line <= skipTo)
          return null;
      } else if (line >= skipFrom)
        // SKIP IT
        return null;
    }

    log(OETLProcessor.LOG_LEVELS.DEBUG, "parsing=%s", input);

    final List<String> fields = OStringSerializerHelper.smartSplit(input.toString(), new char[] { separator }, 0, -1, false, false,
        false, false);

    if (columnNames == null) {
      if (!columnsOnFirstLine)
        throw new OTransformException(getName() + ": columnsOnFirstLine=false and no columns declared");
      columnNames = fields;

      // REMOVE ANY STRING CHARACTERS IF ANY
      for (int i = 0; i < columnNames.size(); ++i)
        columnNames.set(i, OStringSerializerHelper.getStringContent(columnNames.get(i)));

      return null;
    }

    final ODocument doc = new ODocument();
    for (int i = 0; i < columnNames.size() && i < fields.size(); ++i) {
      final String fieldName = columnNames.get(i);
      Object fieldValue = null;
      try {
        final String fieldStringValue = fields.get(i);

        final OType fieldType = columnTypes != null ? columnTypes.get(i) : null;

        if (fieldType != null && fieldType != OType.ANY) {
          // DEFINED TYPE
          fieldValue = OStringSerializerHelper.getStringContent(fieldStringValue);
          try {
            fieldValue = OType.convert(fieldValue, fieldType.getDefaultJavaType());
            doc.field(fieldName, fieldValue);
          } catch (Exception e) {
            processor.getStats().incrementErrors();
            log(OETLProcessor.LOG_LEVELS.ERROR, "Error on converting row %d field '%s' (%d), value '%s' (class:%s) to type: %s",
                processor.getExtractor().getProgress(), fieldName, i, fieldValue, fieldValue.getClass().getName(), fieldType);
          }
        } else if (fieldStringValue != null && !fieldStringValue.isEmpty()) {
          // DETERMINE THE TYPE
          final char firstChar = fieldStringValue.charAt(0);
          if (firstChar == stringCharacter) {
              // STRING
              fieldValue = OStringSerializerHelper.getStringContent(fieldStringValue);
          }
          else if (Character.isDigit(firstChar)) {
              // NUMBER
              if (fieldStringValue.contains(".") || fieldStringValue.contains(",")) {
                  fieldValue = Float.parseFloat(fieldStringValue);
                  if (!Float.isFinite((Float) fieldValue)) {
                      fieldValue = Double.parseDouble(fieldStringValue);
                  }
              } else
                  try {
                      fieldValue = Integer.parseInt(fieldStringValue);
                  } catch (Exception e) {
                      fieldValue = Long.parseLong(fieldStringValue);
                  }
          }
          else
            fieldValue = fieldStringValue;

          if (nullValue != null && nullValue.equals(fieldValue))
            // NULL VALUE, SKIP
            continue;

          doc.field(fieldName, fieldValue);
        }

      } catch (Exception e) {
        processor.getStats().incrementErrors();
        log(OETLProcessor.LOG_LEVELS.ERROR, "Error on setting document field %s=%s (cause=%s)", fieldName, fieldValue, e.toString());
      }
    }

    log(OETLProcessor.LOG_LEVELS.DEBUG, "document=%s", doc);

    return doc;
  }
}
