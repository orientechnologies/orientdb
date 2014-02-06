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
package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.collate.ODefaultCollate;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.type.ODocumentWrapperNoClass;

/**
 * Abstract index definiton implementation.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public abstract class OAbstractIndexDefinition extends ODocumentWrapperNoClass implements OIndexDefinition {
  protected OCollate collate = new ODefaultCollate();

  protected OAbstractIndexDefinition() {
    super(new ODocument());
  }

  public OCollate getCollate() {
    return collate;
  }

  public void setCollate(final OCollate collate) {
    if (collate == null)
      throw new IllegalArgumentException("COLLATE cannot be null");
    this.collate = collate;
  }

  public void setCollate(String iCollate) {
    if (iCollate == null)
      iCollate = ODefaultCollate.NAME;

    setCollate(OSQLEngine.getCollate(iCollate));
  }
}
