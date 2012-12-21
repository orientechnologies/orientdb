/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.db.tool;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;

/**
 * Abstract class for import/export of database and data in general.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public abstract class ODatabaseImpExpAbstract {
  protected ODatabaseRecord        database;
  protected String                 fileName;

  protected Set<String>            includeClusters;
  protected Set<String>            excludeClusters;
  protected Set<String>            includeClasses;
  protected Set<String>            excludeClasses;
  protected boolean                includeInfo               = true;
  protected boolean                includeClusterDefinitions = true;
  protected boolean                includeSchema             = true;
  protected boolean                includeSecurity           = false;
  protected boolean                includeRecords            = true;
  protected boolean                includeIndexDefinitions   = true;
  protected boolean                includeManualIndexes      = true;

  protected OCommandOutputListener listener;

  protected final static String    DEFAULT_EXT               = ".json";

  public ODatabaseImpExpAbstract(final ODatabaseRecord iDatabase, final String iFileName, final OCommandOutputListener iListener) {
    database = iDatabase;
    fileName = iFileName;

    if (fileName != null && fileName.indexOf('.') == -1)
      fileName += DEFAULT_EXT;

    listener = iListener;
    excludeClusters = new LinkedHashSet<String>();
    excludeClusters.add("index");
    excludeClusters.add("manindex");
  }

  public ODatabaseImpExpAbstract setOptions(final String iOptions) {
    if (iOptions != null) {
      final List<String> options = OStringSerializerHelper.smartSplit(iOptions, ' ');
      for (String o : options) {
        final int sep = o.indexOf('=');
        if (sep == -1) {
          OLogManager.instance().warn(this, "Unrecognized option %s, skipped", o);
          continue;
        }

        final String option = o.substring(0, sep);
        final List<String> items = OStringSerializerHelper.smartSplit(o.substring(sep + 1), ' ');

        if (option.equalsIgnoreCase("-includeClass")) {
          includeClasses = new HashSet<String>();
          for (String item : items)
            includeClasses.add(item.toUpperCase());

        } else if (option.equalsIgnoreCase("-excludeClass")) {
          excludeClasses = new HashSet<String>(items);
          for (String item : items)
            excludeClasses.add(item.toUpperCase());

        } else if (option.equalsIgnoreCase("-includeCluster")) {
          includeClusters = new HashSet<String>(items);
          for (String item : items)
            includeClusters.add(item.toUpperCase());

        } else if (option.equalsIgnoreCase("-excludeCluster")) {
          excludeClusters = new HashSet<String>(items);
          for (String item : items)
            excludeClusters.add(item.toUpperCase());

        } else if (option.equalsIgnoreCase("-includeInfo")) {
          includeInfo = Boolean.parseBoolean(items.get(0));

        } else if (option.equalsIgnoreCase("-includeClusterDefinitions")) {
          includeClusterDefinitions = Boolean.parseBoolean(items.get(0));

        } else if (option.equalsIgnoreCase("-includeSchema")) {
          includeSchema = Boolean.parseBoolean(items.get(0));

        } else if (option.equalsIgnoreCase("-includeSecurity")) {
          includeSecurity = Boolean.parseBoolean(items.get(0));

        } else if (option.equalsIgnoreCase("-includeRecords")) {
          includeRecords = Boolean.parseBoolean(items.get(0));

        } else if (option.equalsIgnoreCase("-includeIndexDefinitions")) {
          includeIndexDefinitions = Boolean.parseBoolean(items.get(0));

        } else if (option.equalsIgnoreCase("-includeManualIndexes")) {
          includeManualIndexes = Boolean.parseBoolean(items.get(0));

        }
      }
    }
    return this;
  }

  public Set<String> getIncludeClusters() {
    return includeClusters;
  }

  public void setIncludeClusters(Set<String> includeClusters) {
    this.includeClusters = includeClusters;
  }

  public Set<String> getExcludeClusters() {
    return excludeClusters;
  }

  public void setExcludeClusters(Set<String> excludeClusters) {
    this.excludeClusters = excludeClusters;
  }

  public Set<String> getIncludeClasses() {
    return includeClasses;
  }

  public void setIncludeClasses(Set<String> includeClasses) {
    this.includeClasses = includeClasses;
  }

  public Set<String> getExcludeClasses() {
    return excludeClasses;
  }

  public void setExcludeClasses(Set<String> excludeClasses) {
    this.excludeClasses = excludeClasses;
  }

  public OCommandOutputListener getListener() {
    return listener;
  }

  public void setListener(OCommandOutputListener listener) {
    this.listener = listener;
  }

  public ODatabaseRecord getDatabase() {
    return database;
  }

  public String getFileName() {
    return fileName;
  }

  public boolean isIncludeSchema() {
    return includeSchema;
  }

  public void setIncludeSchema(boolean includeSchema) {
    this.includeSchema = includeSchema;
  }

  public boolean isIncludeRecords() {
    return includeRecords;
  }

  public void setIncludeRecords(boolean includeRecords) {
    this.includeRecords = includeRecords;
  }

  public boolean isIncludeIndexDefinitions() {
    return includeIndexDefinitions;
  }

  public void setIncludeIndexDefinitions(boolean includeIndexDefinitions) {
    this.includeIndexDefinitions = includeIndexDefinitions;
  }

  public boolean isIncludeManualIndexes() {
    return includeManualIndexes;
  }

  public void setIncludeManualIndexes(boolean includeManualIndexes) {
    this.includeManualIndexes = includeManualIndexes;
  }

  public boolean isIncludeClusterDefinitions() {
    return includeClusterDefinitions;
  }

  public void setIncludeClusterDefinitions(boolean includeClusterDefinitions) {
    this.includeClusterDefinitions = includeClusterDefinitions;
  }
}
