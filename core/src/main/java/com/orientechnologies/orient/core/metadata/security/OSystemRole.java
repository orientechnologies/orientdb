/*
 *
 *  *  Copyright 2016 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.annotation.OBeforeDeserialization;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.List;

/**
 */
public class OSystemRole extends ORole {
  public static final String DB_FILTER = "dbFilter";
	
  private List<String> dbFilter;
  public List<String> getDbFilter() { return dbFilter; }
	
  /**
   * Constructor used in unmarshalling.
   */
  public OSystemRole() {
  }

  public OSystemRole(final String iName, final ORole iParent, final ALLOW_MODES iAllowMode) {
  	 super(iName, iParent, iAllowMode);
  }

  /**
   * Create the role by reading the source document.
   */
  public OSystemRole(final ODocument iSource) {
    super(iSource);
  }

  @Override
  @OBeforeDeserialization
  public void fromStream(final ODocument iSource) {
    super.fromStream(iSource);

    if (document != null && document.containsField(DB_FILTER) && document.fieldType(DB_FILTER) == OType.EMBEDDEDLIST) {
    	dbFilter = document.field(DB_FILTER, OType.EMBEDDEDLIST);
    }
  }
}
