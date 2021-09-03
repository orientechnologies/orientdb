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

import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.collate.ODefaultCollate;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OSQLEngine;

/**
 * Abstract index definiton implementation.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public abstract class OAbstractIndexDefinition implements OIndexDefinition {
  protected OCollate collate = new ODefaultCollate();
  private boolean nullValuesIgnored = true;
  protected ODocument document;

  protected OAbstractIndexDefinition() {
    document = new ODocument().setTrackingChanges(false);
  }

  public OCollate getCollate() {
    return collate;
  }

  public void setCollate(final OCollate collate) {
    if (collate == null) throw new IllegalArgumentException("COLLATE cannot be null");
    this.collate = collate;
  }

  public void setCollate(String iCollate) {
    if (iCollate == null) iCollate = ODefaultCollate.NAME;

    setCollate(OSQLEngine.getCollate(iCollate));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    OAbstractIndexDefinition that = (OAbstractIndexDefinition) o;

    if (!collate.equals(that.collate)) return false;

    if (nullValuesIgnored != that.nullValuesIgnored) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = collate.hashCode();
    result = 31 * result + (nullValuesIgnored ? 1 : 0);
    return result;
  }

  @Override
  public boolean isNullValuesIgnored() {
    return nullValuesIgnored;
  }

  @Override
  public void setNullValuesIgnored(boolean value) {
    nullValuesIgnored = value;
  }

  protected void serializeToStream() {}

  protected void serializeFromStream() {}
}
