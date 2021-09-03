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

package com.orientechnologies.orient.object.jpa.parsing;

import java.util.Locale;

public enum PersistenceXml {
  TAG_PERSISTENCE("persistence"),
  TAG_PERSISTENCE_UNIT("persistence-unit"),
  TAG_PROPERTIES("properties"),
  TAG_PROPERTY("property"),
  TAG_NON_JTA_DATA_SOURCE("non-jta-data-source"),
  TAG_JTA_DATA_SOURCE("jta-data-source"),
  TAG_CLASS("class"),
  TAG_MAPPING_FILE("mapping-file"),
  TAG_JAR_FILE("jar-file"),
  TAG_EXCLUDE_UNLISTED_CLASSES("exclude-unlisted-classes"),
  TAG_VALIDATION_MODE("validation-mode"),
  TAG_SHARED_CACHE_MODE("shared-cache-mode"),
  TAG_PROVIDER("provider"),
  TAG_UNKNOWN$("unknown$"),
  ATTR_UNIT_NAME("name"),
  ATTR_TRANSACTION_TYPE("transaction-type"),
  ATTR_SCHEMA_VERSION("version");

  private final String name;

  PersistenceXml(String name) {
    this.name = name;
  }

  /**
   * Case ignorance, null safe method
   *
   * @param aName
   * @return true if tag equals to enum item
   */
  public boolean equals(String aName) {
    return name.equalsIgnoreCase(aName);
  }

  /**
   * Try to parse tag to enum item
   *
   * @param aName
   * @return TAG_UNKNOWN$ if failed to parse
   */
  public static PersistenceXml parse(String aName) {
    try {
      return valueOf("TAG_" + aName.replace('-', '_').toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException e) {
      return TAG_UNKNOWN$;
    }
  }

  @Override
  public String toString() {
    return name;
  }
}
