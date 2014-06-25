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

package com.orientechnologies.orient.etl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.db.tool.importer.listener.OImporterListener;
import com.orientechnologies.orient.core.db.tool.importer.listener.OScriptImporterListener;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.filter.OSQLPredicate;

public class OImporterConfiguration {
  private final Map<String, OImporterConfiguraionField>    fields          = new HashMap<String, OImporterConfiguraionField>();
  private final Map<String, String>                        edges           = new HashMap<String, String>();
  private final Map<OSQLPredicate, OImporterConfiguration> subTemplates    = new HashMap<OSQLPredicate, OImporterConfiguration>();
  private final Map<String[], String[]>                    classIndexes    = new HashMap<String[], String[]>();
  private String                                           className;
  private String                                           superClassName;
  private int                                              mandatoryFields = 0;
  private volatile OImporterListener                       implementation;
  private boolean                                          autoSave            = true;
  private OPair<String, String>                            key             = null;

  public class OImporterConfiguraionField {
    private String        name;
    private OType         type;
    private boolean       optional = false;
    private String        join;
    private String        index;
    private String        format;
    private boolean       trim = false;
    private OSQLPredicate overwriteIf;
  }

  public OImporterConfiguration(final OImporterConfiguration iParent, final ODocument iConfiguration) {
    implementation = iParent.getImplementation();
    parse(iConfiguration);
  }

  public void parse(final ODocument iConfiguration) {
    final Map<String, String> events = new HashMap<String, String>();

    className = iConfiguration.field("class");
    if (className == null)
      throw new OConfigurationException("'class' field not found in configuration");

    superClassName = iConfiguration.field("superClass");

    final String cfgImplementation = iConfiguration.field("implementation");
    if( cfgImplementation!=null )
    try {
      implementation =  (OImporterListener) Class.forName(cfgImplementation).newInstance();
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on creating 'implementation' instance, class '"+cfgImplementation+"'");
      throw new OConfigurationException("Cannot create an object of 'implementation' instance, class '" + cfgImplementation + "'", e);
    }

    final Boolean cfgSave = iConfiguration.field("autoSave");
    if( cfgSave!=null)
      autoSave = cfgSave;

    final ODocument cfgFields = iConfiguration.field("fields");

    for( String fieldName : cfgFields.fieldNames() ){
      final OImporterConfiguraionField field = new OImporterConfiguraionField();
      fields.put(fieldName, field);

      final ODocument cfgField = cfgFields.field(fieldName);

      final String cfgFieldType = cfgField.field("type");
      if( cfgFieldType == null )
            throw new OConfigurationException("field type missed");

      if (!cfgFieldType.equalsIgnoreCase("SKIP"))
        field.type = OType.valueOf(cfgFieldType.toUpperCase());

      if( cfgField.containsField("trim") )
        field.trim = cfgField.field("trim");

      if( cfgField.containsField("join") ){
        field.join = cfgField.field("join");

        if (field.join == null)
          throw new OConfigurationException("Target join field missed");
      }

      if( cfgField.containsField("format") ){
        field.format = cfgField.field("format");

        if (field.format == null)
          throw new OConfigurationException("Format detail missed");

        field.format = OStringSerializerHelper.getStringContent(field.format);
      }

      if( cfgField.containsField("optional") )
        field.optional = cfgField.field("optional");


              } else if (additional.equalsIgnoreCase("@key")) {
                final String keyType = getParameter(parts, ++par);
                if (keyType == null)
                  throw new OConfigurationException("@key type missed. Use 'overwrite' or 'onlyfirst'");
                key = new OPair<String, String>(getParameter(parts, 1), keyType);

              } else if (additional.equalsIgnoreCase("@index")) {
                final String name = getParameter(parts, 1);
                final String type = par < parts.size() - 1 ? getParameter(parts, ++par) : "unique";

                fieldIndexes.put(name, type);

              } else if (additional.equalsIgnoreCase("@edge")) {
                edges.put(getParameter(parts, 1), getParameter(parts, 2));

              } else if (additional.equalsIgnoreCase("@overwriteif")) {
                final OSQLPredicate cond = new OSQLPredicate(getParameter(parts, 1) + " "
                    + OStringSerializerHelper.getStringContent(getParameter(parts, ++par)));
                overwriteIf.put(getParameter(parts, 1), cond);

              } else if (additional.startsWith("#")) {
                // COMMENT, IGNORE THE EST OF THE LINE
                break;

              } else
                throw new OConfigurationException("Additional tag " + additional + " not supported");
            }
          }

        } else if (fieldTag.startsWith("@on")) {
          // EVENTS
          final StringBuilder code = new StringBuilder();

          String codeLine = iConfiguration.next().trim();
          while (iConfiguration.hasNext()) {
            code.append(codeLine);
            code.append("\n");
            codeLine = iConfiguration.next().trim();

            if (codeLine.trim().equals("}}}"))
              break;
          }

          events.put(fieldTag.substring(1), code.toString());

        } else if (fieldTag.equalsIgnoreCase("@if")) {
          // SUB-TEMPLATE
          final String expr = buffer.substring(3);
          subTemplates.put(new OSQLPredicate(expr), new OImporterConfiguration(this, iConfiguration));

        } else if (fieldTag.equalsIgnoreCase("@endif")) {
          // SUB-TEMPLATE
          return;

        } else if (fieldTag.equalsIgnoreCase("@index")) {
          final String name = getParameter(parts, 1);
          final String type = getParameter(parts, 2);

          final String[] fields = new String[parts.size() - 3];
          for (int i = 3; i < parts.size(); ++i) {
            fields[i - 3] = parts.get(i);
          }

          classIndexes.put(new String[] { name, type }, fields);

        } else
          throw new OConfigurationException("Tag " + fieldTag + " not supported");

      } // ELSE IGNORE IT

    }

    if (implementation == null)
      implementation = new OScriptImporterListener(events);
    validate();
  }

  public Set<String> getTrims() {
    return trims;
  }

  public int getMandatoryFields() {
    return mandatoryFields;
  }

  public String getFieldName(final int i) {
    return fieldNames.get(i);
  }

  public OType getFieldType(final String iName) {
    return fieldTypes.get(iName);
  }

  public int getFieldId(final String iName) {
    for (int i = 0; i < fieldNames.size(); ++i)
      if (fieldNames.get(i).equals(iName))
        return i;
    return -1;
  }

  public String getFieldTargetJoin(final String iFieldName) {
    return joins.get(iFieldName);
  }

  public String getFieldFormatDetail(final String iFieldName) {
    return formats.get(iFieldName);
  }

  public String getFieldIndex(final String iFieldName) {
    return fieldIndexes.get(iFieldName);
  }

  public Iterable<Entry<String[], String[]>> getClassIndexes() {
    return classIndexes.entrySet();
  }

  public String getFieldEdge(final String iFieldName) {
    return edges.get(iFieldName);
  }

  public String getClassName() {
    return className;
  }

  public void setClassName(final String className) {
    this.className = className;
  }

  public String getSuperClass() {
    return extendsClass;
  }

  public List<String> getFieldNames() {
    return fieldNames;
  }

  public OPair<String, String> getKey() {
    return key;
  }

  public void setKey(OPair<String, String> key) {
    this.key = key;
  }

  public boolean isFieldOptional(final String iName) {
    return fieldOptionals.contains(iName);
  }

  public void setFieldOptional(final String iName) {
    fieldOptionals.add(iName);
  }

  public Map<OSQLPredicate, OImporterConfiguration> getSubTemplates() {
    return subTemplates;
  }

  public boolean hasSubTemplates() {
    return !subTemplates.isEmpty();
  }

  public boolean isSave() {
    return save;
  }

  public void setSave(boolean save) {
    this.save = save;
  }

  public OImporterListener getImplementation() {
    return implementation;
  }

  public void setImplementation(OImporterListener implementation) {
    this.implementation = implementation;
  }

  public Map<String, OSQLPredicate> getOverwriteIf() {
    return overwriteIf;
  }

  private void validate() {
    boolean optional = false;
    for (String f : fieldNames) {
      if (isFieldOptional(f)) {
        if (!optional)
          optional = true;
      } else if (optional)
        throw new OConfigurationException("The field '" + f
            + "' cannot be defined non-optional after an optional. Optional fields must go after all");
      else
        mandatoryFields++;
    }
  }

  private String getParameter(final List<String> parts, final int i) {
    return parts.size() > i ? parts.get(i) : null;
  }
}
