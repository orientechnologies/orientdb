package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.config.OStorageEntryConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OStorage;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Returns an OResult containing metadata regarding the storage
 *
 * @author Luigi Dell'Aquila (l.dellaquila - at - orientdb.com)
 */
public class FetchFromStorageMetadataStep extends AbstractExecutionStep {

  private boolean served = false;
  private long cost = 0;

  public FetchFromStorageMetadataStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    return new OResultSet() {
      @Override
      public boolean hasNext() {
        return !served;
      }

      @Override
      public OResult next() {
        long begin = profilingEnabled ? System.nanoTime() : 0;
        try {

          if (!served) {
            OResultInternal result = new OResultInternal();

            if (ctx.getDatabase() instanceof ODatabaseInternal) {
              ODatabaseInternal db = (ODatabaseInternal) ctx.getDatabase();
              OStorage storage = db.getStorage();
              result.setProperty("clusters", toResult(storage.getClusterInstances()));
              result.setProperty("defaultClusterId", storage.getDefaultClusterId());
              result.setProperty("totalClusters", storage.getClusters());
              result.setProperty("configuration", toResult(storage.getConfiguration()));
              result.setProperty(
                  "conflictStrategy",
                  storage.getRecordConflictStrategy() == null
                      ? null
                      : storage.getRecordConflictStrategy().getName());
              result.setProperty("name", storage.getName());
              result.setProperty("size", storage.getSize());
              result.setProperty("type", storage.getType());
              result.setProperty("version", storage.getVersion());
              result.setProperty("createdAtVersion", storage.getCreatedAtVersion());
            }
            served = true;
            return result;
          }
          throw new IllegalStateException();
        } finally {
          if (profilingEnabled) {
            cost += (System.nanoTime() - begin);
          }
        }
      }

      @Override
      public void close() {}

      @Override
      public Optional<OExecutionPlan> getExecutionPlan() {
        return Optional.empty();
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return null;
      }

      @Override
      public void reset() {
        served = false;
      }
    };
  }

  private Object toResult(OStorageConfiguration configuration) {
    OResultInternal result = new OResultInternal();
    result.setProperty("charset", configuration.getCharset());
    result.setProperty("clusterSelection", configuration.getClusterSelection());
    result.setProperty("conflictStrategy", configuration.getConflictStrategy());
    result.setProperty("dateFormat", configuration.getDateFormat());
    result.setProperty("dateTimeFormat", configuration.getDateTimeFormat());
    result.setProperty("localeCountry", configuration.getLocaleCountry());
    result.setProperty("localeLanguage", configuration.getLocaleLanguage());
    result.setProperty("recordSerializer", configuration.getRecordSerializer());
    result.setProperty("timezone", String.valueOf(configuration.getTimeZone()));
    result.setProperty("properties", toResult(configuration.getProperties()));
    return result;
  }

  private List<OResult> toResult(List<OStorageEntryConfiguration> properties) {
    List<OResult> result = new ArrayList<>();
    if (properties != null) {
      for (OStorageEntryConfiguration entry : properties) {
        OResultInternal item = new OResultInternal();
        item.setProperty("name", entry.name);
        item.setProperty("value", entry.value);
        result.add(item);
      }
    }
    return result;
  }

  private List<OResult> toResult(Collection<? extends OCluster> clusterInstances) {
    List<OResult> result = new ArrayList<>();
    if (clusterInstances != null) {
      for (OCluster cluster : clusterInstances) {
        OResultInternal item = new OResultInternal();
        item.setProperty("name", cluster.getName());
        item.setProperty("fileName", cluster.getFileName());
        item.setProperty("id", cluster.getId());
        item.setProperty("entries", cluster.getEntries());
        item.setProperty(
            "conflictStrategy",
            cluster.getRecordConflictStrategy() == null
                ? null
                : cluster.getRecordConflictStrategy().getName());
        item.setProperty("tombstonesCount", cluster.getTombstonesCount());
        try {
          item.setProperty("encryption", cluster.encryption());
        } catch (Exception e) {
          OLogManager.instance().error(this, "Can not set value of encryption parameter", e);
        }
        result.add(item);
      }
    }
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ FETCH STORAGE METADATA";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }

  @Override
  public long getCost() {
    return cost;
  }
}
