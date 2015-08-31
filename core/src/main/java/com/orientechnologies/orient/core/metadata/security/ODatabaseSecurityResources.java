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
 * Contains all the resources used by the Database instance to check permissions
 * 
 * @author Luca Garulli
 * @see OUser, OSecurity
 */
public class ODatabaseSecurityResources {

  public final static String ALL               = "*";
  public final static String DATABASE          = "database";
  public final static String SCHEMA            = "database.schema";
  public final static String CLASS             = "database.class";
  public final static String ALL_CLASSES       = "database.class.*";
  public final static String CLUSTER           = "database.cluster";
  public final static String ALL_CLUSTERS      = "database.cluster.*";
  public final static String SYSTEMCLUSTERS    = "database.systemclusters";
  public final static String COMMAND           = "database.command";
  public final static String FUNCTION          = "database.function";
  public final static String DATABASE_CONFIG   = "database.config";
  public final static String BYPASS_RESTRICTED = "database.bypassRestricted";
  public final static String RECORD_HOOK       = "database.hook.record";
  public final static String SERVER_ADMIN      = "server.admin";
}
