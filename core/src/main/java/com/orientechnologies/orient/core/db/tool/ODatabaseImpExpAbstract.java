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
package com.orientechnologies.orient.core.db.tool;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Abstract class for import/export of database and data in general.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public abstract class ODatabaseImpExpAbstract extends ODatabaseTool {
  protected static final String DEFAULT_EXT = ".json";
  protected ODatabaseDocumentInternal database;
  protected String fileName;
  protected Set<String> includeClusters;
  protected Set<String> excludeClusters;
  protected Set<String> includeClasses;
  protected Set<String> excludeClasses;
  protected boolean includeInfo = true;
  protected boolean includeClusterDefinitions = true;
  protected boolean includeSchema = true;
  protected boolean includeSecurity = false;
  protected boolean includeRecords = true;
  protected boolean includeIndexDefinitions = true;
  protected boolean includeManualIndexes = true;
  protected boolean useLineFeedForRecords = false;
  protected boolean preserveRids = false;
  protected OCommandOutputListener listener;

  public ODatabaseImpExpAbstract(
      final ODatabaseDocumentInternal iDatabase,
      final String iFileName,
      final OCommandOutputListener iListener) {
    database = iDatabase;
    fileName = iFileName;

    // Fix bug where you can't backup files with spaces. Now you can wrap with quotes and the
    // filesystem won't create
    // directories with quotes in their name.
    if (fileName != null) {
      if ((fileName.startsWith("\"") && fileName.endsWith("\""))
          || (fileName.startsWith("'") && fileName.endsWith("'"))) {
        fileName = fileName.substring(1, fileName.length() - 1);
        iListener.onMessage("Detected quotes surrounding filename; new backup file: " + fileName);
      }
    }

    if (fileName != null && fileName.indexOf('.') == -1) fileName += DEFAULT_EXT;

    listener = iListener;
    excludeClusters = new LinkedHashSet<>();
    excludeClusters.add(OMetadataDefault.CLUSTER_INDEX_NAME);
    excludeClusters.add(OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME);
  }

  public Set<String> getIncludeClusters() {
    return includeClusters;
  }

  public void setIncludeClusters(final Set<String> includeClusters) {
    this.includeClusters = includeClusters;
  }

  public Set<String> getExcludeClusters() {
    return excludeClusters;
  }

  public void setExcludeClusters(final Set<String> excludeClusters) {
    this.excludeClusters = excludeClusters;
  }

  public Set<String> getIncludeClasses() {
    return includeClasses;
  }

  public void setIncludeClasses(final Set<String> includeClasses) {
    this.includeClasses = includeClasses;
  }

  public Set<String> getExcludeClasses() {
    return excludeClasses;
  }

  public void setExcludeClasses(final Set<String> excludeClasses) {
    this.excludeClasses = excludeClasses;
  }

  public OCommandOutputListener getListener() {
    return listener;
  }

  public void setListener(final OCommandOutputListener listener) {
    this.listener = listener;
  }

  public ODatabaseDocument getDatabase() {
    return database;
  }

  public String getFileName() {
    return fileName;
  }

  public boolean isIncludeInfo() {
    return includeInfo;
  }

  public void setIncludeInfo(final boolean includeInfo) {
    this.includeInfo = includeInfo;
  }

  public boolean isIncludeSecurity() {
    return includeSecurity;
  }

  public void setIncludeSecurity(final boolean includeSecurity) {
    this.includeSecurity = includeSecurity;
  }

  public boolean isIncludeSchema() {
    return includeSchema;
  }

  public void setIncludeSchema(final boolean includeSchema) {
    this.includeSchema = includeSchema;
  }

  public boolean isIncludeRecords() {
    return includeRecords;
  }

  public void setIncludeRecords(final boolean includeRecords) {
    this.includeRecords = includeRecords;
  }

  public boolean isIncludeIndexDefinitions() {
    return includeIndexDefinitions;
  }

  public void setIncludeIndexDefinitions(final boolean includeIndexDefinitions) {
    this.includeIndexDefinitions = includeIndexDefinitions;
  }

  public boolean isIncludeManualIndexes() {
    return includeManualIndexes;
  }

  public void setIncludeManualIndexes(final boolean includeManualIndexes) {
    this.includeManualIndexes = includeManualIndexes;
  }

  public boolean isIncludeClusterDefinitions() {
    return includeClusterDefinitions;
  }

  public void setIncludeClusterDefinitions(final boolean includeClusterDefinitions) {
    this.includeClusterDefinitions = includeClusterDefinitions;
  }

  public boolean isUseLineFeedForRecords() {
    return useLineFeedForRecords;
  }

  public void setUseLineFeedForRecords(final boolean useLineFeedForRecords) {
    this.useLineFeedForRecords = useLineFeedForRecords;
  }

  public boolean isPreserveRids() {
    return preserveRids;
  }

  public void setPreserveRids(boolean preserveRids) {
    this.preserveRids = preserveRids;
  }

  protected void parseSetting(final String option, final List<String> items) {
    if (option.equalsIgnoreCase("-excludeAll")) {
      includeInfo = false;
      includeClusterDefinitions = false;
      includeSchema = false;
      includeSecurity = false;
      includeRecords = false;
      includeIndexDefinitions = false;
      includeManualIndexes = false;

    } else if (option.equalsIgnoreCase("-includeClass")) {
      includeClasses = new HashSet<String>();
      for (String item : items) includeClasses.add(item.toUpperCase(Locale.ENGLISH));
      includeRecords = true;

    } else if (option.equalsIgnoreCase("-excludeClass")) {
      excludeClasses = new HashSet<String>(items);
      for (String item : items) excludeClasses.add(item.toUpperCase(Locale.ENGLISH));

    } else if (option.equalsIgnoreCase("-includeCluster")) {
      includeClusters = new HashSet<String>(items);
      for (String item : items) includeClusters.add(item.toUpperCase(Locale.ENGLISH));
      includeRecords = true;

    } else if (option.equalsIgnoreCase("-excludeCluster")) {
      excludeClusters = new HashSet<String>(items);
      for (String item : items) excludeClusters.add(item.toUpperCase(Locale.ENGLISH));

    } else if (option.equalsIgnoreCase("-includeInfo")) {
      includeInfo = Boolean.parseBoolean(items.get(0));

    } else if (option.equalsIgnoreCase("-includeClusterDefinitions")) {
      includeClusterDefinitions = Boolean.parseBoolean(items.get(0));

    } else if (option.equalsIgnoreCase("-includeSchema")) {
      includeSchema = Boolean.parseBoolean(items.get(0));
      if (includeSchema) {
        includeClusterDefinitions = true;
        includeInfo = true;
      }
    } else if (option.equalsIgnoreCase("-includeSecurity")) {
      includeSecurity = Boolean.parseBoolean(items.get(0));

    } else if (option.equalsIgnoreCase("-includeRecords")) {
      includeRecords = Boolean.parseBoolean(items.get(0));

    } else if (option.equalsIgnoreCase("-includeIndexDefinitions")) {
      includeIndexDefinitions = Boolean.parseBoolean(items.get(0));

    } else if (option.equalsIgnoreCase("-includeManualIndexes")) {
      includeManualIndexes = Boolean.parseBoolean(items.get(0));

    } else if (option.equalsIgnoreCase("-useLineFeedForRecords")) {
      useLineFeedForRecords = Boolean.parseBoolean(items.get(0));
    }
  }
}
