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
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.List;

/**
 */
public class OSystemUser extends OUser {
  private String databaseName;
  protected String getDatabaseName() { return databaseName; }

  /**
   * Constructor used in unmarshalling.
   */
  public OSystemUser() {
  }

  public OSystemUser(final String iName) {
    super(iName);
  }

  public OSystemUser(String iUserName, final String iUserPassword) {
    super(iUserName, iUserPassword);
  }

  /**
   * Create the user by reading the source document.
   */
  public OSystemUser(final ODocument iSource) {
    super(iSource);
  }

  /**
   * dbName is the name of the source database and is used for filtering roles.
   */
  public OSystemUser(final ODocument iSource, final String dbName) {
    databaseName = dbName;
    fromStream(iSource);
  }

  /**
   * Derived classes can override createRole() to return an extended ORole implementation.
   */
  protected ORole createRole(final ODocument roleDoc) {
    ORole role = null; 

    // If databaseName is set, then only allow roles with the same databaseName.
    if (databaseName != null && !databaseName.isEmpty()) {
    	if (roleDoc != null && roleDoc.containsField(OSystemRole.DB_FILTER) && roleDoc.fieldType(OSystemRole.DB_FILTER) == OType.EMBEDDEDLIST) {
    		
    	  List<String> dbNames = roleDoc.field(OSystemRole.DB_FILTER, OType.EMBEDDEDLIST);
  	 	  
  	 	  for (String dbName : dbNames) {
  	       if (dbName != null && !dbName.isEmpty() && (dbName.equalsIgnoreCase(databaseName) || dbName.equals("*"))) {
    	      role = new OSystemRole(roleDoc);
    	      break;
    	    }
    	  }
      }
    }
    // If databaseName is not set, only return roles without a OSystemRole.DB_FILTER property.
    else {
      if (roleDoc != null && !roleDoc.containsField(OSystemRole.DB_FILTER)) {
    	  role = new OSystemRole(roleDoc);
      }
    }

    return role;
  }
}
