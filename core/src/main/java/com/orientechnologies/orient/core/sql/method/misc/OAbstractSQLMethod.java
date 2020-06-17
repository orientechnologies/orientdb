/*
 * Copyright 2013 Geomatys.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.sql.method.misc;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.method.OSQLMethod;

/** @author Johann Sorel (Geomatys) */
public abstract class OAbstractSQLMethod implements OSQLMethod {

  private final String name;
  private final int minparams;
  private final int maxparams;

  public OAbstractSQLMethod(String name) {
    this(name, 0);
  }

  public OAbstractSQLMethod(String name, int nbparams) {
    this(name, nbparams, nbparams);
  }

  public OAbstractSQLMethod(String name, int minparams, int maxparams) {
    this.name = name;
    this.minparams = minparams;
    this.maxparams = maxparams;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getSyntax() {
    final StringBuilder sb = new StringBuilder("<field>.");
    sb.append(getName());
    sb.append('(');
    for (int i = 0; i < minparams; i++) {
      if (i != 0) {
        sb.append(", ");
      }
      sb.append("param");
      sb.append(i + 1);
    }
    if (minparams != maxparams) {
      sb.append('[');
      for (int i = minparams; i < maxparams; i++) {
        if (i != 0) {
          sb.append(", ");
        }
        sb.append("param");
        sb.append(i + 1);
      }
      sb.append(']');
    }
    sb.append(')');

    return sb.toString();
  }

  @Override
  public int getMinParams() {
    return minparams;
  }

  @Override
  public int getMaxParams() {
    return maxparams;
  }

  protected Object getParameterValue(final OIdentifiable iRecord, final String iValue) {
    if (iValue == null) {
      return null;
    }

    if (iValue.charAt(0) == '\'' || iValue.charAt(0) == '"') {
      // GET THE VALUE AS STRING
      return iValue.substring(1, iValue.length() - 1);
    }

    if (iRecord == null) {
      return null;
    }
    // SEARCH FOR FIELD
    return ((ODocument) iRecord.getRecord()).field(iValue);
  }

  @Override
  public int compareTo(OSQLMethod o) {
    return this.getName().compareTo(o.getName());
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public boolean evaluateParameters() {
    return true;
  }
}
