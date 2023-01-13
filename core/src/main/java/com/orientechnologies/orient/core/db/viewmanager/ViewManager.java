package com.orientechnologies.orient.core.db.viewmanager;

import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OLiveQueryResultListener;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentEmbedded;
import com.orientechnologies.orient.core.index.OClassIndexManager;
import com.orientechnologies.orient.core.index.OClassIndexManager.IndexChange;
import com.orientechnologies.orient.core.index.OCompositeIndexDefinition;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexDefinitionFactory;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.index.OPropertyMapIndexDefinition.INDEX_BY;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.schema.OView;
import com.orientechnologies.orient.core.metadata.schema.OViewConfig;
import com.orientechnologies.orient.core.metadata.schema.OViewConfig.OViewIndexConfig.OIndexConfigProperty;
import com.orientechnologies.orient.core.metadata.schema.OViewImpl;
import com.orientechnologies.orient.core.metadata.schema.OViewRemovedMetadata;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.parser.OProjection;
import com.orientechnologies.orient.core.sql.parser.OProjectionItem;
import com.orientechnologies.orient.core.sql.parser.OSelectStatement;
import com.orientechnologies.orient.core.sql.parser.OStatement;
import com.orientechnologies.orient.core.sql.parser.OStatementCache;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class ViewManager {
  private final OrientDBInternal orientDB;
  private final String dbName;
  private boolean viewsExist = false;

  /**
   * To retain clusters that are being used in queries until the queries are closed.
   *
   * <p>view -> cluster -> number of visitors
   */
  private final ConcurrentMap<Integer, AtomicInteger> viewCluserVisitors =
      new ConcurrentHashMap<>();

  private final ConcurrentMap<Integer, String> oldClustersPerViews = new ConcurrentHashMap<>();
  private final List<Integer> clustersToDrop = Collections.synchronizedList(new ArrayList<>());

  /**
   * To retain indexes that are being used in queries until the queries are closed.
   *
   * <p>view -> index -> number of visitors
   */
  private final ConcurrentMap<String, AtomicInteger> viewIndexVisitors = new ConcurrentHashMap<>();

  private final ConcurrentMap<String, String> oldIndexesPerViews = new ConcurrentHashMap<>();
  private final List<String> indexesToDrop = Collections.synchronizedList(new ArrayList<>());

  private final ConcurrentMap<String, Long> lastUpdateTimestampForView = new ConcurrentHashMap<>();

  private final ConcurrentMap<String, Long> lastChangePerClass = new ConcurrentHashMap<>();
  private final Set<String> refreshing = Collections.synchronizedSet(new HashSet<>());

  private volatile TimerTask timerTask;
  private volatile Future<?> lastTask;
  private volatile boolean closed = false;

  public ViewManager(OrientDBInternal orientDb, String dbName) {
    this.orientDB = orientDb;
    this.dbName = dbName;
  }

  protected void init() {
    orientDB.executeNoAuthorization(
        dbName,
        (db) -> {
          // do this to make sure that the storage is already initialized and so is the shared
          // context.
          // you just don't need the db passed as a param here
          registerLiveUpdates(db);
          return null;
        });
  }

  private void registerLiveUpdates(ODatabaseSession db) {
    boolean liveViewsExist = false;
    Collection<OView> views = db.getMetadata().getSchema().getViews();
    for (OView view : views) {
      liveViewsExist = registerLiveUpdateFor(db, view.getName()) || liveViewsExist;
    }
  }

  public boolean registerLiveUpdateFor(ODatabaseSession db, String viewName) {
    OView view = db.getMetadata().getSchema().getView(viewName);
    viewsExist = true;
    boolean registered = false;
    if (view.getUpdateStrategy() != null
        && view.getUpdateStrategy().equalsIgnoreCase(OViewConfig.UPDATE_STRATEGY_LIVE)) {
      db.live(view.getQuery(), new ViewUpdateListener(view.getName()));
      registered = true;
    }

    return registered;
  }

  public void load() {
    closed = false;
    init();
    start();
  }

  public void create() {
    closed = false;
    init();
    start();
  }

  public void start() {
    schedule();
  }

  private void schedule() {
    this.timerTask =
        new TimerTask() {
          @Override
          public void run() {
            if (closed) return;
            lastTask =
                orientDB.executeNoAuthorization(
                    dbName,
                    (db) -> {
                      ViewManager.this.updateViews((ODatabaseDocumentInternal) db);
                      return null;
                    });
          }
        };
    this.orientDB.schedule(timerTask, 1000, 1000);
  }

  private void updateViews(ODatabaseDocumentInternal db) {
    try {
      cleanUnusedViewClusters(db);
      cleanUnusedViewIndexes(db);
      OView view = getNextViewToUpdate(db);
      if (view != null) {
        updateView(view, db);
      }

    } catch (Exception e) {
      OLogManager.instance().warn(this, "Failed to update views", e);
    }
  }

  public void close() {
    if (timerTask != null) {
      timerTask.cancel();
    }
    if (lastTask != null) {
      try {
        // Try to cancel last task before it runs, otherwise wait for completion
        if (!lastTask.cancel(false)) {
          lastTask.get(20, TimeUnit.SECONDS);
        }
      } catch (TimeoutException e) {
        OLogManager.instance()
            .warn(
                this,
                "Timeout waiting for last task to complete view update background operations");
      } catch (InterruptedException e) {
        throw OException.wrapException(
            new OInterruptedException("Terminated while waiting update view to finis"), e);
      } catch (ExecutionException e) {
        // Will already have been logged by thread pool executor
      }
    }

    closed = true;
  }

  public void cleanUnusedViewClusters(ODatabaseDocument db) {
    if (((ODatabaseDocumentEmbedded) db).getStorage().isIcrementalBackupRunning()) {
      // backup is running handle delete the next run
      return;
    }
    List<Integer> toRemove = new ArrayList<>();
    for (Integer cluster : clustersToDrop) {
      AtomicInteger visitors = viewCluserVisitors.get(cluster);
      if (visitors == null || visitors.get() <= 0) {
        toRemove.add(cluster);
      }
    }

    for (Integer cluster : toRemove) {
      db.dropCluster(cluster);
      viewCluserVisitors.remove(cluster);
      clustersToDrop.remove(cluster);
      oldClustersPerViews.remove(cluster);
    }
  }

  public void cleanUnusedViewIndexes(ODatabaseDocumentInternal db) {
    if (((ODatabaseDocumentEmbedded) db).getStorage().isIcrementalBackupRunning()) {
      // backup is running handle delete the next run
      return;
    }

    List<String> toRemove = new ArrayList<>();
    for (String index : indexesToDrop) {
      AtomicInteger visitors = viewIndexVisitors.get(index);
      if (visitors == null || visitors.get() <= 0) {
        toRemove.add(index);
      }
    }
    for (String index : toRemove) {
      db.getMetadata().getIndexManagerInternal().dropIndex(db, index);
      viewIndexVisitors.remove(index);
      indexesToDrop.remove(index);
      oldIndexesPerViews.remove(index);
    }
  }

  public OView getNextViewToUpdate(ODatabaseDocumentInternal db) {
    OSchema schema = db.getMetadata().getImmutableSchemaSnapshot();

    Collection<OView> views = schema.getViews();

    if (views.isEmpty()) {
      return null;
    }
    for (OView view : views) {
      if (!buildOnThisNode(db, view)) {
        continue;
      }
      if (isLiveUpdate(db, view)) {
        continue;
      }
      if (!isUpdateExpiredFor(view, db)) {
        continue;
      }
      if (!needsUpdateBasedOnWatchRules(view, db)) {
        continue;
      }
      if (isViewRefreshing(view)) {
        continue;
      }
      return db.getMetadata().getSchema().getView(view.getName());
    }

    return null;
  }

  private boolean isViewRefreshing(OView view) {
    return this.refreshing.contains(view.getName());
  }

  private boolean isLiveUpdate(ODatabaseDocumentInternal db, OView view) {
    return OViewConfig.UPDATE_STRATEGY_LIVE.equalsIgnoreCase(view.getUpdateStrategy());
  }

  protected boolean buildOnThisNode(ODatabaseDocumentInternal db, OView name) {
    return true;
  }

  /**
   * Checks if the view could need an update based on watch rules
   *
   * @param view view name
   * @param db db instance
   * @return true if there are no watch rules for this view; true if there are watch rules and some
   *     of them happened since last update; true if the view was never updated; false otherwise.
   */
  private boolean needsUpdateBasedOnWatchRules(OView view, ODatabaseDocumentInternal db) {
    if (view == null) {
      return false;
    }

    Long lastViewUpdate = lastUpdateTimestampForView.get(view.getName());
    if (lastViewUpdate == null) {
      return true;
    }

    List<String> watchRules = view.getWatchClasses();
    if (watchRules == null || watchRules.size() == 0) {
      return true;
    }

    for (String watchRule : watchRules) {
      Long classChangeTimestamp = lastChangePerClass.get(watchRule.toLowerCase(Locale.ENGLISH));
      if (classChangeTimestamp == null) {
        continue;
      }
      if (classChangeTimestamp >= lastViewUpdate) {
        return true;
      }
    }

    return false;
  }

  private boolean isUpdateExpiredFor(OView view, ODatabaseDocumentInternal db) {
    Long lastUpdate = lastUpdateTimestampForView.get(view.getName());
    if (lastUpdate == null) {
      return true;
    }
    int updateInterval = view.getUpdateIntervalSeconds();
    return lastUpdate + (updateInterval * 1000) < System.currentTimeMillis();
  }

  public void updateView(OView view, ODatabaseDocumentInternal db) {
    OScenarioThreadLocal.executeAsDistributed(
        () -> {
          this.updateViewInternal(view, db);
          return null;
        });
  }

  public void updateViewInternal(OView view, ODatabaseDocumentInternal db) {
    if (((ODatabaseDocumentEmbedded) db).getStorage().isIcrementalBackupRunning()) {
      // backup is running handle rebuild the next run
      return;
    }
    try {
      refreshing.add(view.getName());

      OLogManager.instance().info(this, "Starting refresh of view '%s'", view.getName());
      lastUpdateTimestampForView.put(view.getName(), System.currentTimeMillis());
      int cluster = db.addCluster(getNextClusterNameFor(view, db));

      String viewName = view.getName();
      String query = view.getQuery();
      String originRidField = view.getOriginRidField();
      String clusterName = db.getClusterNameById(cluster);

      List<OIndex> indexes = createNewIndexesForView(view, cluster, db);

      OScenarioThreadLocal.executeAsDistributed(
          new Callable<Object>() {
            @Override
            public Object call() {
              int iterationCount = 0;
              db.begin();
              try (OResultSet rs = db.query(query)) {
                while (rs.hasNext()) {
                  OResult item = rs.next();
                  addItemToView(item, db, originRidField, viewName, clusterName, indexes);
                  if (iterationCount % 100 == 0) {
                    db.commit();
                  }
                }
              }
              db.commit();
              return null;
            }
          });

      view = db.getMetadata().getSchema().getView(view.getName());
      if (view == null) {
        // the view was dropped in the meantime
        clustersToDrop.add(cluster);
        indexes.forEach(x -> indexesToDrop.add(x.getName()));
        return;
      }
      OViewRemovedMetadata oldMetadata =
          ((OViewImpl) view).replaceViewClusterAndIndex(cluster, indexes);
      for (int i : oldMetadata.getClusters()) {
        clustersToDrop.add(i);
        viewCluserVisitors.put(i, new AtomicInteger(0));
        oldClustersPerViews.put(i, view.getName());
      }

      oldMetadata
          .getIndexes()
          .forEach(
              idx -> {
                indexesToDrop.add(idx);
                viewIndexVisitors.put(idx, new AtomicInteger(0));
                oldIndexesPerViews.put(idx, viewName);
              });
      cleanUnusedViewIndexes(db);
      cleanUnusedViewClusters(db);
      OLogManager.instance().info(this, "Finished refresh of view '%s'", view.getName());
    } finally {
      refreshing.remove(view.getName());
    }
  }

  private void addItemToView(
      OResult item,
      ODatabaseDocument db,
      String originRidField,
      String viewName,
      String clusterName,
      List<OIndex> indexes) {
    db.begin();
    OElement newRow = copyElement(item, db);
    if (originRidField != null) {
      newRow.setProperty(originRidField, item.getIdentity().orElse(item.getProperty("@rid")));
      newRow.setProperty("@view", viewName);
    }
    db.save(newRow, clusterName);
    List<IndexChange> ops = new ArrayList<>();
    OClassIndexManager.addIndexesEntries(
        (ODatabaseDocumentInternal) db, (ODocument) newRow, indexes, ops);
    OClassIndexManager.applyChanges(ops);
    db.commit();
  }

  private List<OIndex> createNewIndexesForView(
      OView view, int cluster, ODatabaseDocumentInternal db) {
    try {
      List<OIndex> result = new ArrayList<>();
      OIndexManagerAbstract idxMgr = db.getMetadata().getIndexManagerInternal();
      for (OViewConfig.OViewIndexConfig cfg : view.getRequiredIndexesInfo()) {
        OIndexDefinition definition = createIndexDefinition(view.getName(), cfg.getProperties());
        String indexName = view.getName() + "_" + UUID.randomUUID().toString().replaceAll("-", "_");
        String type = cfg.getType();
        String engine = cfg.getEngine();
        OIndex idx =
            idxMgr.createIndex(
                db, indexName, type, definition, new int[] {cluster}, null, null, engine);
        result.add(idx);
      }
      return result;
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  private OIndexDefinition createIndexDefinition(
      String viewName, List<OIndexConfigProperty> requiredIndexesInfo) {
    if (requiredIndexesInfo.size() == 1) {
      String fieldName = requiredIndexesInfo.get(0).getName();
      OType fieldType = requiredIndexesInfo.get(0).getType();
      OType linkedType = requiredIndexesInfo.get(0).getLinkedType();
      OCollate collate = requiredIndexesInfo.get(0).getCollate();
      INDEX_BY index_by = requiredIndexesInfo.get(0).getIndexBy();
      return OIndexDefinitionFactory.createSingleFieldIndexDefinition(
          viewName, fieldName, fieldType, linkedType, collate, null, index_by);
    }
    OCompositeIndexDefinition result = new OCompositeIndexDefinition(viewName);
    for (OIndexConfigProperty pair : requiredIndexesInfo) {
      String fieldName = pair.getName();
      OType fieldType = pair.getType();
      OType linkedType = pair.getLinkedType();
      OCollate collate = pair.getCollate();
      INDEX_BY index_by = pair.getIndexBy();
      OIndexDefinition definition =
          OIndexDefinitionFactory.createSingleFieldIndexDefinition(
              viewName, fieldName, fieldType, linkedType, collate, null, index_by);
      result.addIndex(definition);
    }
    return result;
  }

  private String getNextClusterNameFor(OView view, ODatabase db) {
    int i = 0;
    String viewName = view.getName();
    while (true) {
      String clusterName = viewName.toLowerCase(Locale.ENGLISH) + (i++);
      if (!db.getClusterNames().contains(clusterName)) {
        return clusterName;
      }
    }
  }

  private OElement copyElement(OResult item, ODatabaseDocument db) {
    OElement newRow = db.newElement();
    for (String prop : item.getPropertyNames()) {
      if (!prop.equalsIgnoreCase("@rid") && !prop.equalsIgnoreCase("@class")) {
        newRow.setProperty(prop, item.getProperty(prop));
      }
    }
    return newRow;
  }

  public void updateViewAsync(String name, ViewCreationListener listener) {
    orientDB.executeNoAuthorization(
        dbName,
        (databaseSession) -> {
          if (!buildOnThisNode(
              (ODatabaseDocumentInternal) databaseSession,
              ((ODatabaseDocumentInternal) databaseSession)
                  .getMetadata()
                  .getSchema()
                  .getView(name))) {
            return null;
          }
          try {
            OView view = databaseSession.getMetadata().getSchema().getView(name);
            updateView(view, (ODatabaseDocumentInternal) databaseSession);
            if (listener != null) {
              listener.afterCreate(databaseSession, name);
            }
          } catch (Exception e) {
            if (listener != null) {
              listener.onError(name, e);
            }
          }
          return null;
        });
  }

  public void startUsingViewCluster(Integer cluster) {
    AtomicInteger item = viewCluserVisitors.get(cluster);
    if (item == null) {
      item = new AtomicInteger(0);
      viewCluserVisitors.put(cluster, item);
    }
    item.incrementAndGet();
  }

  public void endUsingViewCluster(Integer cluster) {
    AtomicInteger item = viewCluserVisitors.get(cluster);
    if (item == null) {
      return;
    }
    item.decrementAndGet();
  }

  public void recordAdded(
      OImmutableClass clazz, ODocument doc, ODatabaseDocumentEmbedded oDatabaseDocumentEmbedded) {
    if (viewsExist) {
      lastChangePerClass.put(
          clazz.getName().toLowerCase(Locale.ENGLISH), System.currentTimeMillis());
    }
  }

  public void recordUpdated(
      OImmutableClass clazz, ODocument doc, ODatabaseDocumentEmbedded oDatabaseDocumentEmbedded) {
    if (viewsExist) {
      lastChangePerClass.put(
          clazz.getName().toLowerCase(Locale.ENGLISH), System.currentTimeMillis());
    }
  }

  public void recordDeleted(
      OImmutableClass clazz, ODocument doc, ODatabaseDocumentEmbedded oDatabaseDocumentEmbedded) {
    if (viewsExist) {
      lastChangePerClass.put(
          clazz.getName().toLowerCase(Locale.ENGLISH), System.currentTimeMillis());
    }
  }

  public String getViewFromOldCluster(int clusterId) {
    return oldClustersPerViews.get(clusterId);
  }

  public void endUsingViewIndex(String indexName) {
    AtomicInteger item = viewIndexVisitors.get(indexName);
    if (item == null) {
      return;
    }
    item.decrementAndGet();
  }

  public void startUsingViewIndex(String indexName) {
    AtomicInteger item = viewIndexVisitors.get(indexName);
    if (item == null) {
      item = new AtomicInteger(0);
      viewIndexVisitors.put(indexName, item);
    }
    item.incrementAndGet();
  }

  private class ViewUpdateListener implements OLiveQueryResultListener {
    private final String viewName;

    public ViewUpdateListener(String name) {
      this.viewName = name;
    }

    @Override
    public void onCreate(ODatabaseDocument db, OResult data) {
      OView view = db.getMetadata().getSchema().getView(viewName);
      if (view != null) {
        int cluster = view.getClusterIds()[0];
        addItemToView(
            data,
            db,
            view.getOriginRidField(),
            view.getName(),
            db.getClusterNameById(cluster),
            new ArrayList<>(view.getIndexes()));
      }
    }

    @Override
    public void onUpdate(ODatabaseDocument db, OResult before, OResult after) {
      OView view = db.getMetadata().getSchema().getView(viewName);
      if (view != null && view.getOriginRidField() != null) {
        try (OResultSet rs =
            db.query(
                "SELECT FROM " + viewName + " WHERE " + view.getOriginRidField() + " = ?",
                (Object) after.getProperty("@rid"))) {
          while (rs.hasNext()) {
            OResult row = rs.next();
            row.getElement()
                .ifPresent(
                    elem -> updateViewRow(elem, after, view, (ODatabaseDocumentInternal) db));
          }
        }
      }
    }

    private void updateViewRow(
        OElement viewRow, OResult origin, OView view, ODatabaseDocumentInternal db) {
      OStatement stm = OStatementCache.get(view.getQuery(), db);
      if (stm instanceof OSelectStatement) {
        OProjection projection = ((OSelectStatement) stm).getProjection();
        if (projection == null
            || (projection.getItems().size() == 0 && projection.getItems().get(0).isAll())) {
          for (String s : origin.getPropertyNames()) {
            if ("@rid".equalsIgnoreCase(s)
                || "@class".equalsIgnoreCase(s)
                || "@version".equalsIgnoreCase(s)) {
              continue;
            }
            Object value = origin.getProperty(s);
            viewRow.setProperty(s, value);
          }
        } else {
          for (OProjectionItem oProjectionItem : projection.getItems()) {
            Object value = oProjectionItem.execute(origin, new OBasicCommandContext());
            viewRow.setProperty(oProjectionItem.getProjectionAliasAsString(), value);
          }
        }
        viewRow.save();
      }
    }

    @Override
    public void onDelete(ODatabaseDocument db, OResult data) {
      OView view = db.getMetadata().getSchema().getView(viewName);
      if (view != null && view.getOriginRidField() != null) {
        try (OResultSet rs =
            db.query(
                "SELECT FROM " + viewName + " WHERE " + view.getOriginRidField() + " = ?",
                (Object) data.getProperty("@rid"))) {
          while (rs.hasNext()) {
            rs.next().getElement().ifPresent(x -> x.delete());
          }
        }
      }
    }

    @Override
    public void onError(ODatabaseDocument database, OException exception) {
      OLogManager.instance().error(ViewManager.this, "Error updating view " + viewName, exception);
    }

    @Override
    public void onEnd(ODatabaseDocument database) {}
  }
}
