/*
 *
 *  * Copyright 2010-2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.orient.etl;

public class OImporterData {
  public String databaseName;
  public String databaseUser;
  public String databasePassword;
  public String importFileExtension;
  public String separator;
  public String templatePath;
  public String templateFileExtension;
  public long   delay;
  public String nullValue;
  public int    dumpProgressEvery;

  public OImporterData(String databaseUser, String databasePassword, String templateFileExtension, String nullValue,
      int dumpProgressEvery) {
    this.databaseUser = databaseUser;
    this.databasePassword = databasePassword;
    this.templateFileExtension = templateFileExtension;
    this.nullValue = nullValue;
    this.dumpProgressEvery = dumpProgressEvery;
  }
}
