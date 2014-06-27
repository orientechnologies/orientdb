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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.etl.listener.OImporterListener;
import com.orientechnologies.orient.etl.listener.OScriptImporterListener;

import java.util.HashMap;
import java.util.Map;

public class OTransformerConfiguration {
  private final Map<String, OImporterConfiguraionField>       fields       = new HashMap<String, OImporterConfiguraionField>();
  private String className;
  private String superClassName;
  private volatile OImporterListener implementation;
  private boolean               autoSave = true;

  public enum KEY_TYPE {
    OVERWRITE, ONLYFIRST
  }

  public class OImporterConfiguraionField {
    // private OSQLPredicate overwriteIf;
    public  String edgeClass;
    public  String onValue;
    private String name;
    private OType  type;
    private boolean optional = false;
    private String   join;
    private KEY_TYPE key;
    private String   index;
    private String   format;
    private String   transform;

    public boolean isOptional() {
      return optional;
    }
  }

  public OTransformerConfiguration(final OTransformerConfiguration iParent, final ODocument iConfiguration) {
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
    if (cfgImplementation != null)
      try {
        implementation = (OImporterListener) Class.forName(cfgImplementation).newInstance();
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error on creating 'implementation' instance, class '" + cfgImplementation + "'");
        throw new OConfigurationException(
            "Cannot create an object of 'implementation' instance, class '" + cfgImplementation + "'", e);
      }

    final Boolean cfgSave = iConfiguration.field("autoSave");
    if (cfgSave != null)
      autoSave = cfgSave;

    final ODocument cfgFields = iConfiguration.field("fields");

    for (String fieldName : cfgFields.fieldNames()) {
      final OImporterConfiguraionField field = new OImporterConfiguraionField();
      fields.put(fieldName, field);

      final ODocument cfgField = cfgFields.field(fieldName);

      final String cfgFieldType = cfgField.field("type");
      if (cfgFieldType == null)
        throw new OConfigurationException("field type missed");

      if (!cfgFieldType.equalsIgnoreCase("skip"))
        field.type = OType.valueOf(cfgFieldType.toUpperCase());

      if (cfgField.containsField("transform"))
        field.transform = cfgField.field("transform");

      if (cfgField.containsField("join")) {
        field.join = cfgField.field("join");

        if (field.join == null)
          throw new OConfigurationException("Target join field missed");
      }

      if (cfgField.containsField("format")) {
        field.format = cfgField.field("format");

        if (field.format == null)
          throw new OConfigurationException("Format detail missed");

        field.format = OStringSerializerHelper.getStringContent(field.format);
      }

      if (cfgField.containsField("optional"))
        field.optional = cfgField.field("optional");

      if (cfgField.containsField("key")) {
        try {
          field.key = KEY_TYPE.valueOf(((String) cfgField.field("key")).toUpperCase());
        } catch (IllegalArgumentException e) {
          throw new OConfigurationException("key type missed. Use 'overwrite' or 'onlyfirst'");
        }
      }

      if (cfgField.containsField("index")) {
        field.index = ((String) cfgField.field("index")).toUpperCase();
        if (field.index == null)
          throw new OConfigurationException("index type missed. Use the available types, like 'unique' or 'notunique'");
      }

      field.edgeClass = cfgField.field("edgeClass");

      if (cfgField.containsField("onValue"))
        field.onValue = cfgField.field("onValue");

      // if (additional.equalsIgnoreCase("@overwriteif")) {
      // final OSQLPredicate cond = new OSQLPredicate(getParameter(parts, 1) + " "
      // + OStringSerializerHelper.getStringContent(getParameter(parts, ++par)));
      // overwriteIf.put(getParameter(parts, 1), cond);
      //
      // }

    }

    // PARSE EVENTS
    final ODocument cfgEvents = iConfiguration.field("events");
    for (String eventName : cfgEvents.fieldNames()) {
      events.put(eventName, (String) cfgEvents.field(eventName));
    }

    if (implementation == null)
      implementation = new OScriptImporterListener(events);
  }

  public String getClassName() {
    return className;
  }

  public boolean isAutoSave() {
    return autoSave;
  }

  public void setAutoSave(final boolean save) {
    this.autoSave = save;
  }

  public OImporterListener getImplementation() {
    return implementation;
  }

  public void setImplementation(final OImporterListener implementation) {
    this.implementation = implementation;
  }
}
