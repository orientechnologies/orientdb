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

import com.orientechnologies.agent.services.backup.log.OBackupUploadFinishedLog;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Optional;

/**
 * It has the responsibility to upload a local backup to a remote destination according to a certain strategy.
 */

public class OLocalBackupUploader {

  private OUploadingStrategy uploadingStrategy;
  private static final OUploadingStrategyFactory FACTORY = new OUploadingStrategyFactory();

  public OLocalBackupUploader(String uploadingStrategy) {
    this.uploadingStrategy = FACTORY.buildStrategy(uploadingStrategy);
  }

  public static Optional<OLocalBackupUploader> from(ODocument cfg) {
    if (cfg == null) {
      return Optional.empty();
    }
    String uploadStrategy = cfg.field("strategy");
    if (uploadStrategy == null) {
      OLogManager.instance().warn(null, "Cannot configure the cloud uploader, strategy parameters is missing", null);
      return Optional.empty();
    }
    OLocalBackupUploader uploader = new OLocalBackupUploader(uploadStrategy);
    try {
      uploader.config(cfg);
    } catch (Exception e) {
      OLogManager.instance().warn(uploader, "Cannot configure the cloud uploader", e);
      return Optional.empty();
    }
    return Optional.of(uploader);
  }

  /*

   */
  public boolean executeUpload(String sourceBackupDirectory, String destinationDirectoryPath, String... accessParameters) {
    return this.uploadingStrategy.executeUpload(sourceBackupDirectory, destinationDirectoryPath, accessParameters);
  }

  public OUploadMetadata executeUpload(String sourceFile, String fname, String destinationDirectoryPath) {
    return this.uploadingStrategy.executeUpload(sourceFile, fname, destinationDirectoryPath);
  }

  protected void config(ODocument cfg) {
    this.uploadingStrategy.config(cfg);
  }

  public String executeDownload(OBackupUploadFinishedLog upload) {
    return this.uploadingStrategy.executeDownload(upload);
  }

}
