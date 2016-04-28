/*
 * Copyright 2016 OrientDB LTD (info(at)orientdb.com)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 *   For more information: http://www.orientdb.com
 */

package com.orientechnologies.backup.uploader;

import com.orientechnologies.orient.core.exception.OConfigurationException;

/**
 * Factory used to instantiate the chosen strategy for the uploading of he local backup.
 */
public class OUploadingStrategyFactory {

  public OUploadingStrategy buildStrategy(String uploadingStrategyName) {

    OUploadingStrategy uploadingStrategy;

    if(uploadingStrategyName.equals("s3")) {
      uploadingStrategy = new OS3DeltaUploadingStrategy();
    }
    else if(uploadingStrategyName.equals("sftp")) {
      uploadingStrategy = new OSFTPDeltaUploadingStrategy();
    }
    else {
      throw new OConfigurationException("Uploading strategy no supported. Accepted values: s3, sftp.");
    }

    return uploadingStrategy;

  }
}
