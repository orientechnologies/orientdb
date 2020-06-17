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
package com.orientechnologies.orient.core.config;

import com.orientechnologies.common.util.OCommonConst;
import java.io.Serializable;

@SuppressWarnings("serial")
public class OStorageSegmentConfiguration implements Serializable {
  public transient OStorageConfiguration root;
  public volatile int id;
  public volatile String name;
  public volatile String maxSize = "0";
  public volatile String fileType = "mmap";
  public volatile String fileStartSize = "500Kb";
  public volatile String fileMaxSize = "500Mb";
  public volatile String fileIncrementSize = "50%";
  public volatile String defrag = "auto";
  public volatile STATUS status = STATUS.ONLINE;
  public OStorageFileConfiguration[] infoFiles;
  protected String location;

  public enum STATUS {
    ONLINE,
    OFFLINE
  }

  public OStorageSegmentConfiguration() {
    infoFiles = OCommonConst.EMPTY_FILE_CONFIGURATIONS_ARRAY;
  }

  public void setRoot(OStorageConfiguration iRoot) {
    this.root = iRoot;
    for (OStorageFileConfiguration f : infoFiles) f.parent = this;
  }

  public String getLocation() {
    if (location != null) return location;

    return root != null ? root.getDirectory() : null;
  }
}
