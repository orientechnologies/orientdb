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
package com.orientechnologies.orient.core.metadata.security;

/**
 * Contains all the resources used by the Database instance to check permissions
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 * @see OUser, OSecurity
 */
public class ODatabaseSecurityResources {

  public static final String ALL = "*";
  public static final String DATABASE = "database";
  public static final String SCHEMA = "database.schema";
  public static final String CLASS = "database.class";
  public static final String ALL_CLASSES = "database.class.*";
  public static final String CLUSTER = "database.cluster";
  public static final String ALL_CLUSTERS = "database.cluster.*";
  public static final String SYSTEMCLUSTERS = "database.systemclusters";
  public static final String COMMAND = "database.command";
  public static final String COMMAND_GREMLIN = "database.command.gremlin";
  public static final String FUNCTION = "database.function";
  public static final String DATABASE_CONFIG = "database.config";
  public static final String BYPASS_RESTRICTED = "database.bypassRestricted";
  public static final String RECORD_HOOK = "database.hook.record";
  public static final String SERVER_ADMIN = "server.admin";
}
