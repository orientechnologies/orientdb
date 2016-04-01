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

/**
 * It has the responsibility to upload a local backup to a remote destination according to a certain strategy.
 */

public class OLocalBackupUploader {

  private OUploadingStrategy uploadingStrategy;
  private static final OUploadingStrategyFactory FACTORY = new OUploadingStrategyFactory();

  public OLocalBackupUploader(String uploadingStrategy) {
    this.uploadingStrategy = FACTORY.buildStrategy(uploadingStrategy);
  }

  /*

   */
  public boolean executeUpload(String sourceBackupDirectory, String destinationDirectoryPath, String... accessParameters) {
    return this.uploadingStrategy.executeUpload(sourceBackupDirectory, destinationDirectoryPath, accessParameters);
  }

}
