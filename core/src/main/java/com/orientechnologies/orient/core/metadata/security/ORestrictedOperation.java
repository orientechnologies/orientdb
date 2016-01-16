/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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

/**
 * Enum containing the restricted security (Record Level Security) permissions.
 * 
 * @author Luca Garulli
 * 
 */
public enum ORestrictedOperation {
  /**
   * Allows all RUD rights.
   */
  ALLOW_ALL("_allow"),

  /**
   * Allows Read rights.
   */
  ALLOW_READ("_allowRead"),

  /**
   * Allows Update rights.
   */
  ALLOW_UPDATE("_allowUpdate"),

  /**
   * Allows Delete rights.
   */
  ALLOW_DELETE("_allowDelete");

  private final String fieldName;

  ORestrictedOperation(final String iFieldName) {
    fieldName = iFieldName;
  }

  public String getFieldName() {
    return fieldName;
  }
}
