/*
 *  *  Copyright 2016 OrientDB LTD (info(at)orientdb.com)
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *          http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  *  For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.storage.impl.local.statistic;

import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Container for cluster related performance numbers
 */
public class OClusterCountersHolder extends OSessionStoragePerformanceStatistic.PerformanceCountersHolder {
  /**
   * Amount of all created records.
   */
  protected long createdRecords;

  /**
   * Total time is needed to create all records
   */
  protected long timeRecordCreation;

  private long createRecordPages;

  private long createRecordFilePages;

  private long createRecordPageReadTime;

  private long createRecordFilePageReadTime;

  /**
   * Amount of all deleted records
   */
  protected long deletedRecords;

  /**
   * Total time is needed to delete all records
   */
  protected long timeRecordDeletion;

  private long deleteRecordPages;

  private long deleteRecordFilePages;

  private long deleteRecordPageReadTime = 0;

  private long deleteRecordFilePageReadTime = 0;

  /**
   * Amount of all updated records
   */
  protected long updatedRecords;

  /**
   * Total time which is needed to update all records
   */
  protected long timeRecordUpdate;

  private long updateRecordPages;

  private long updateRecordFilePages;

  private long updateRecordPageReadTime;

  private long updateRecordFilePageReadTime;

  /**
   * Amount of all read records
   */
  protected long readRecords;

  /**
   * Total time which is needed to read all records.
   */
  protected long timeRecordRead;

  private long readRecordPages;

  private long readRecordFilePages;

  private long readRecordPageReadTime;

  private long readRecordFilePageReadTime;

  @Override
  public OClusterCountersHolder newInstance() {
    return new OClusterCountersHolder();
  }

  @Override
  public void clean() {
    super.clean();

    createdRecords = 0;
    timeRecordCreation = 0;
    createRecordPages = 0;
    createRecordFilePages = 0;
    createRecordPageReadTime = 0;
    createRecordFilePageReadTime = 0;

    deletedRecords = 0;
    timeRecordDeletion = 0;
    deleteRecordPages = 0;
    deleteRecordFilePages = 0;
    deleteRecordPageReadTime = 0;
    deleteRecordFilePageReadTime = 0;

    updatedRecords = 0;
    timeRecordUpdate = 0;
    updateRecordPages = 0;
    updateRecordFilePages = 0;
    updateRecordPageReadTime = 0;
    updateRecordFilePageReadTime = 0;

    readRecords = 0;
    timeRecordRead = 0;
    readRecordPages = 0;
    readRecordFilePages = 0;
    readRecordPageReadTime = 0;
    readRecordFilePageReadTime = 0;
  }

  public long getRecordCreationTime() {
    if (createdRecords == 0)
      return -1;

    return timeRecordCreation / createdRecords;
  }

  public long getRecordCreationPages() {
    if (createdRecords == 0)
      return -1;

    return createRecordPages / createdRecords;
  }

  public int getRecordCreationHitRate() {
    if (createRecordPages == 0)
      return -1;

    return (int) ((100 * (createRecordPages - createRecordFilePages)) / createRecordPages);
  }

  public long getRecordCreationPageTime() {
    if (createRecordPages == 0)
      return -1;

    return createRecordPageReadTime / createRecordPages;
  }

  public long getRecordCreationFilePageTime() {
    if (createRecordFilePages == 0)
      return -1;

    return createRecordFilePageReadTime / createRecordFilePages;
  }

  public long getRecordDeletionTime() {
    if (deletedRecords == 0)
      return -1;

    return timeRecordDeletion / deletedRecords;
  }

  public long getRecordDeletionPages() {
    if (deletedRecords == 0)
      return -1;

    return deleteRecordPages / deletedRecords;
  }

  public long getRecordDeletionHitRate() {
    if (deleteRecordPages == 0)
      return -1;

    return (int) (100 * (deleteRecordPages - deleteRecordFilePages) / deleteRecordPages);
  }

  public long getRecordDeletionPageTime() {
    if (deleteRecordPages == 0)
      return -1;

    return deleteRecordPageReadTime / deleteRecordPages;
  }

  public long getRecordDeletionFilePageTime() {
    if (deleteRecordFilePages == 0)
      return -1;

    return deleteRecordFilePageReadTime / deleteRecordFilePages;
  }

  public long getRecordUpdateTime() {
    if (updatedRecords == 0)
      return -1;

    return timeRecordUpdate / updatedRecords;
  }

  public long getRecordUpdatePages() {
    if (updatedRecords == 0)
      return -1;

    return updateRecordPages / updatedRecords;
  }

  public long getRecordUpdateHitRate() {
    if (updateRecordPages == 0)
      return -1;

    return (int) (100 * (updateRecordPages - updateRecordFilePages) / updateRecordPages);
  }

  public long getRecordUpdatePageTime() {
    if (updateRecordPages == 0)
      return -1;

    return updateRecordPageReadTime / updateRecordPages;
  }

  public long getRecordUpdateFilePageTime() {
    if (updateRecordFilePages == 0)
      return -1;

    return updateRecordFilePageReadTime / updateRecordFilePages;
  }

  public long getRecordReadTime() {
    if (readRecords == 0)
      return -1;

    return timeRecordRead / readRecords;
  }

  public long getRecordReadPages() {
    if (readRecords == 0)
      return -1;

    return readRecordPages / readRecords;
  }

  public long getRecordReadHitRate() {
    if (readRecordPages == 0)
      return -1;

    return (int) (100 * (readRecordPages - readRecordFilePages) / readRecordPages);
  }

  public long getRecordReadPageTime() {
    if (readRecordPages == 0)
      return -1;

    return readRecordPageReadTime / readRecordPages;
  }

  public long getRecordReadFilePageTime() {
    if (readRecordFilePages == 0)
      return -1;

    return readRecordFilePageReadTime / readRecordFilePages;
  }

  @Override
  public ODocument toDocument() {
    final ODocument document = super.toDocument();

    OSessionStoragePerformanceStatistic.writeMetric(document,"recordCreationTime", getRecordCreationTime());
    OSessionStoragePerformanceStatistic.writeMetric(document,"recordCreationPages", getRecordCreationPages());
    OSessionStoragePerformanceStatistic.writeMetric(document,"recordCreationHitRate", getRecordCreationHitRate());
    OSessionStoragePerformanceStatistic.writeMetric(document,"recordCreationPageTime", getRecordCreationPageTime());
    OSessionStoragePerformanceStatistic.writeMetric(document,"recordCreationFilePageTime", getRecordCreationFilePageTime());

    OSessionStoragePerformanceStatistic.writeMetric(document,"recordDeletionTime", getRecordDeletionTime());
    OSessionStoragePerformanceStatistic.writeMetric(document,"recordDeletionPages", getRecordDeletionPages());
    OSessionStoragePerformanceStatistic.writeMetric(document,"recordDeletionHitRate", getRecordDeletionHitRate());
    OSessionStoragePerformanceStatistic.writeMetric(document,"recordDeletionPageTime", getRecordDeletionPageTime());
    OSessionStoragePerformanceStatistic.writeMetric(document,"recordDeletionFilePageTime", getRecordDeletionFilePageTime());

    OSessionStoragePerformanceStatistic.writeMetric(document,"recordUpdateTime", getRecordUpdateTime());
    OSessionStoragePerformanceStatistic.writeMetric(document,"recordUpdatePages", getRecordUpdatePages());
    OSessionStoragePerformanceStatistic.writeMetric(document,"recordUpdateHitRate", getRecordUpdateHitRate());
    OSessionStoragePerformanceStatistic.writeMetric(document,"recordUpdatePageTime", getRecordUpdatePageTime());
    OSessionStoragePerformanceStatistic.writeMetric(document,"recordUpdateFilePageTime", getRecordUpdateFilePageTime());

    OSessionStoragePerformanceStatistic.writeMetric(document,"recordReadTime", getRecordReadTime());
    OSessionStoragePerformanceStatistic.writeMetric(document,"recordReadPages", getRecordReadPages());
    OSessionStoragePerformanceStatistic.writeMetric(document,"recordReadHitRate", getRecordReadHitRate());
    OSessionStoragePerformanceStatistic.writeMetric(document,"recordReadPageTime", getRecordReadPageTime());
    OSessionStoragePerformanceStatistic.writeMetric(document,"recordReadFilePageTime", getRecordReadFilePageTime());

    return document;
  }

  @SuppressWarnings("unused")
  public class CreateRecordOperation extends OOperation {
    @Override
    void incrementOperationsCounter(int pages, int filePages) {
      createRecordPages += pages;
      createRecordFilePages += filePages;
    }
  }

  @SuppressWarnings("unused")
  public class UpdateRecordOperation extends OOperation {
    @Override
    void incrementOperationsCounter(int pages, int filePages) {
      OClusterCountersHolder.this.updateRecordPages += pages;
      OClusterCountersHolder.this.updateRecordFilePages += filePages;
    }
  }

  @SuppressWarnings("unused")
  public class DeleteRecordPages extends OOperation {
    @Override
    void incrementOperationsCounter(int pages, int filePages) {
      OClusterCountersHolder.this.deleteRecordPages += pages;
      OClusterCountersHolder.this.deleteRecordFilePages += filePages;
    }
  }

  @SuppressWarnings("unused")
  public class ReadRecordPages extends OOperation {
    @Override
    void incrementOperationsCounter(int pages, int filePages) {
      OClusterCountersHolder.this.readRecordPages += pages;
      OClusterCountersHolder.this.readRecordFilePages += filePages;
    }
  }
}
