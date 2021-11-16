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
package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Set;

/**
 * Contains the index metadata.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OIndexMetadata {
  private final String name;
  private final OIndexDefinition indexDefinition;
  private final Set<String> clustersToIndex;
  private final String type;
  private final String algorithm;
  private final String valueContainerAlgorithm;
  private final ODocument metadata;

  public OIndexMetadata(
      String name,
      OIndexDefinition indexDefinition,
      Set<String> clustersToIndex,
      String type,
      String algorithm,
      String valueContainerAlgorithm,
      ODocument metadata) {
    this.name = name;
    this.indexDefinition = indexDefinition;
    this.clustersToIndex = clustersToIndex;
    this.type = type;
    this.algorithm = algorithm;
    this.valueContainerAlgorithm = valueContainerAlgorithm;
    this.metadata = metadata;
  }

  public String getName() {
    return name;
  }

  public OIndexDefinition getIndexDefinition() {
    return indexDefinition;
  }

  public Set<String> getClustersToIndex() {
    return clustersToIndex;
  }

  public String getType() {
    return type;
  }

  public String getAlgorithm() {
    return algorithm;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    final OIndexMetadata that = (OIndexMetadata) o;

    if (algorithm != null ? !algorithm.equals(that.algorithm) : that.algorithm != null)
      return false;
    if (!clustersToIndex.equals(that.clustersToIndex)) return false;
    if (indexDefinition != null
        ? !indexDefinition.equals(that.indexDefinition)
        : that.indexDefinition != null) return false;
    if (!name.equals(that.name)) return false;
    if (!type.equals(that.type)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + (indexDefinition != null ? indexDefinition.hashCode() : 0);
    result = 31 * result + clustersToIndex.hashCode();
    result = 31 * result + type.hashCode();
    result = 31 * result + (algorithm != null ? algorithm.hashCode() : 0);
    return result;
  }

  String getValueContainerAlgorithm() {
    return valueContainerAlgorithm;
  }

  public ODocument getMetadata() {
    return metadata;
  }
}
