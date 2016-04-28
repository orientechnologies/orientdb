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

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.orientechnologies.common.log.OLogManager;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This strategy performs an upload to a S3 bucket. The upload of the delta between the local backup directory and the remote one is performed.
 *
 * @param
 */

public class OS3DeltaUploadingStrategy implements OUploadingStrategy {

  private final String SUFFIX = "/";

  public OS3DeltaUploadingStrategy() {}


  //

  /**
   * Uploads a backup to a S3 bucket
   *
   * @param sourceBackupDirectory
   * @param destinationDirectoryPath
   * @param accessParameters (String bucketName, String accessKey, String secretKey)
   * @return success
   */
  public boolean executeUpload(String sourceBackupDirectory, String destinationDirectoryPath, String... accessParameters) {

    String bucketName = accessParameters[0];
    String accessKey = accessParameters[1];
    String secretKey = accessParameters[2];

    boolean success = false;
    AWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
    AmazonS3Client s3client = new AmazonS3Client(awsCredentials);

    try {

    /*
     * preparing bucket: if not present it's built
     */

      List<Bucket> buckets = s3client.listBuckets();

      boolean alreadyPresent = false;
      for (Bucket b : buckets) {
        if (b.getName().equals(bucketName)) {
          alreadyPresent = true;
          break;
        }
      }
      // if the bucket is not present build it
      if (!alreadyPresent) {
        s3client.createBucket(bucketName);
      }


    /*
     * uploading file to the bucket
     */

      File localBackupDirectory = new File(sourceBackupDirectory);

      File[] filesLocalBackup = localBackupDirectory.listFiles();
      Map<String, File> localFileName2File = new ConcurrentHashMap<String,File>();
      for(File f: filesLocalBackup) {
        localFileName2File.put(f.getName(), f);
      }

      List<S3ObjectSummary> filesOnBucket = s3client.listObjects(bucketName).getObjectSummaries();
      List<String> remoteFileNames = new ArrayList<String>();

      String currentFileName;
      for(S3ObjectSummary obj: filesOnBucket) {
        remoteFileNames.add(obj.getKey());
      }

      // preparing folder: if folder does not exist  it's created (case: first incremental backup)
      int lastIndex = destinationDirectoryPath.length()-1;
      if(destinationDirectoryPath.charAt(lastIndex) == '/')
        destinationDirectoryPath = destinationDirectoryPath.substring(0,lastIndex);
      if(destinationDirectoryPath.charAt(0) == '/')
        destinationDirectoryPath = destinationDirectoryPath.substring(1);

      if(!(remoteFileNames.contains(destinationDirectoryPath))) {
        this.createFolder(s3client, bucketName, destinationDirectoryPath);
      }

      // compare files in the bucket with the local ones and populate filesToUpload list
      for(String fileName: localFileName2File.keySet()) {
        if(remoteFileNames.contains(destinationDirectoryPath + SUFFIX + fileName)) {
          localFileName2File.remove(fileName);
        }
      }

      // upload each file contained in the filesToUpload list
      for (File currentFile: localFileName2File.values()) {
        s3client.putObject(new PutObjectRequest(bucketName, destinationDirectoryPath + SUFFIX + currentFile.getName(), currentFile));
      }

      success =  true;

    } catch (AmazonServiceException ase) {
      OLogManager.instance().info(this,"Caught an AmazonServiceException, which " +
          "means your request made it " +
          "to Amazon S3, but was rejected with an error response" +
          " for some reason.");
      OLogManager.instance().info(this,"Error Message:    %s", ase.getMessage());
      OLogManager.instance().info(this,"HTTP Status Code: %s", ase.getStatusCode());
      OLogManager.instance().info(this,"AWS Error Code:   %s", ase.getErrorCode());
      OLogManager.instance().info(this,"Error Type:       %s", ase.getErrorType());
      OLogManager.instance().info(this,"Request ID:       %s", ase.getRequestId());
    } catch (AmazonClientException ace) {
      OLogManager.instance().info(this,"Caught an AmazonClientException, which " +
          "means the client encountered " +
          "an internal error while trying to " +
          "communicate with S3, " +
          "such as not being able to access the network.");
      OLogManager.instance().info(this,"Error Message: %s", ace.getMessage());
    } catch (Exception e) {
      OLogManager.instance().info(this,"Caught an exception client side.");
      OLogManager.instance().info(this,"Error Message: %s", e.getMessage());
    }

    return success;
  }

  public void createFolder(AmazonS3Client s3Client, String bucketName, String folderName) {

    // create meta-data for your folder and set content-length to 0
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(0);

    // create empty content
    InputStream emptyContent = new ByteArrayInputStream(new byte[0]);

    // create a PutObjectRequest passing the folder name suffixed by /
    PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, folderName + SUFFIX, emptyContent, metadata);

    // send request to S3 to create folder
    s3Client.putObject(putObjectRequest);

  }
}
