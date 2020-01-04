package com.orientechnologies.orient.core.db.viewmanager;

import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentEmbedded;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.metadata.schema.*;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.parser.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ViewManager {
  private final OrientDBInternal orientDB;
  private final String           dbName;

  /**
   * To retain clusters that are being used in queries until the queries are closed.
   * <p>
   * view -> cluster -> number of visitors
   */
  private final ConcurrentMap<Integer, AtomicInteger> viewCluserVisitors  = new ConcurrentHashMap<>();
  private final ConcurrentMap<Integer, String>        oldClustersPerViews = new ConcurrentHashMap<>();
  private final List<Integer>                         clustersToDrop      = Collections.synchronizedList(new ArrayList<>());

  /**
   * To retain indexes that are being used in queries until the queries are closed.
   * <p>
   * view -> index -> number of visitors
   */
  private final ConcurrentMap<String, AtomicInteger> viewIndexVisitors  = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, String>        oldIndexesPerViews = new ConcurrentHashMap<>();
  private final List<String>                         indexesToDrop      = Collections.synchronizedList(new ArrayList<>());

  private final Map<String, Long> lastUpdateTimestampForView = new HashMap<>();

  private final Map<String, Long> lastChangePerClass = new ConcurrentHashMap<>();

  private volatile String    lastUpdatedView = null;
  private volatile TimerTask timerTask;
  private volatile Future<?> lastTask;
  private volatile boolean   closed          = false;

  public ViewManager(OrientDBInternal orientDb, String dbName) {
    this.orientDB = orientDb;
    this.dbName = dbName;
  }

  protected void init() {
    orientDB.executeNoAuthorization(dbName, (db) -> {
      // do this to make sure that the storage is already initialized and so is the shared context.
      // you just don't need the db passed as a param here
      registerLiveUpdates(db);
      return null;
    });

  }

  private synchronized void registerLiveUpdates(ODatabaseSession db) {
    boolean liveViewsExist = false;
    Collection<OView> views = db.getMetadata().getSchema().getViews();
    for (OView view : views) {
      liveViewsExist = registerLiveUpdateFor(db, view.getName()) || liveViewsExist;
    }
  }

  public synchronized boolean registerLiveUpdateFor(ODatabaseSession db, String viewName) {
    OView view = db.getMetadata().getSchema().getView(viewName);
    boolean registered = false;
    if (view.getUpdateStrategy() != null && view.getUpdateStrategy().equalsIgnoreCase(OViewConfig.UPDATE_STRATEGY_LIVE)) {
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

  public void start() {
    schedule();
  }

  private void schedule() {
    this.timerTask = new TimerTask() {
      @Override
      public void run() {
        if (closed)
          return;
        lastTask = orientDB.executeNoAuthorization(dbName, (db) -> {
          ViewManager.this.updateViews((ODatabaseDocumentInternal) db);
          return null;
        });
      }
    };
    this.orientDB.scheduleOnce(timerTask, 1000);
  }

  private void updateViews(ODatabaseDocumentInternal db) {
    try {
      cleanUnusedViewClusters(db);
      cleanUnusedViewIndexes(db);
      OView view = getNextViewToUpdate(db);
      if (view != null) {
        updateView(view, db);
      }
      //When the run is finished schedule the next run.
      schedule();
    } catch (Exception e) {
      OLogManager.instance().warn(this, "Failed to update views");
      e.printStackTrace();
    }
  }

  public void close() {
    if (timerTask != null) {
      timerTask.cancel();
    }
    if (lastTask != null) {
      try {
        try {
          lastTask.get(20, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
          lastTask.cancel(true);
          lastTask.get();
        }

      } catch (InterruptedException e) {
        throw OException.wrapException(new OInterruptedException("Terminated while waiting update view to finis"), e);
      } catch (ExecutionException e) {
        OLogManager.instance().warn(this, "Issue terminating view update background operations", e);
      }
    }

    closed = true;
  }

  public synchronized void cleanUnusedViewClusters(ODatabaseDocument db) {
    List<Integer> toRemove = new ArrayList<>();
    for (Integer cluster : clustersToDrop) {
      AtomicInteger visitors = viewCluserVisitors.get(cluster);
      if (visitors == null || visitors.get() <= 0) {
        toRemove.add(cluster);
      }
    }
    for (Integer cluster : toRemove) {
      viewCluserVisitors.remove(cluster);
      clustersToDrop.remove(cluster);
      oldClustersPerViews.remove(cluster);
      db.dropCluster(cluster);
    }
  }

  public synchronized void cleanUnusedViewIndexes(ODatabaseDocumentInternal db) {
    List<String> toRemove = new ArrayList<>();
    for (String index : indexesToDrop) {
      AtomicInteger visitors = viewIndexVisitors.get(index);
      if (visitors == null || visitors.get() <= 0) {
        toRemove.add(index);
      }
    }
    for (String index : toRemove) {
      viewIndexVisitors.remove(index);
      indexesToDrop.remove(index);
      oldIndexesPerViews.remove(index);
      db.getMetadata().getIndexManagerInternal().dropIndex(db, index);
    }
  }

  public synchronized OView getNextViewToUpdate(ODatabase db) {
    OSchema schema = db.getMetadata().getSchema();

    List<String> names = schema.getViews().stream().map(x -> x.getName()).sorted().collect(Collectors.toList());
    if (names.isEmpty()) {
      return null;
    }
    for (String name : names) {
      if (!buildOnThisNode(db, name)) {
        continue;
      }
      if (isLiveUpdate(db, name)) {
        continue;
      }
      if (!isUpdateExpiredFor(name, db)) {
        continue;
      }
      if (!needsUpdateBasedOnWatchRules(name, db)) {
        continue;
      }
      if (lastUpdatedView == null || name.compareTo(lastUpdatedView) > 0) {
        lastUpdatedView = name;
        return schema.getView(name);
      }
    }

    lastUpdatedView = null;
    return null;
  }

  private boolean isLiveUpdate(ODatabase db, String viewName) {
    OView view = db.getMetadata().getSchema().getView(viewName);
    return OViewConfig.UPDATE_STRATEGY_LIVE.equalsIgnoreCase(view.getUpdateStrategy());
  }

  protected boolean buildOnThisNode(ODatabase db, String name) {
    return true;
  }

  /**
   * Checks if the view could need an update based on watch rules
   *
   * @param name view name
   * @param db   db instance
   *
   * @return true if there are no watch rules for this view; true if there are watch rules and some of them happened since last
   * update; true if the view was never updated; false otherwise.
   */
  private boolean needsUpdateBasedOnWatchRules(String name, ODatabase db) {
    OView view = db.getMetadata().getSchema().getView(name);
    if (view == null) {
      return false;
    }

    Long lastViewUpdate = lastUpdateTimestampForView.get(name);
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

  private boolean isUpdateExpiredFor(String viewName, ODatabase db) {
    Long lastUpdate = lastUpdateTimestampForView.get(viewName);
    if (lastUpdate == null) {
      return true;
    }
    OView view = db.getMetadata().getSchema().getView(viewName);
    int updateInterval = view.getUpdateIntervalSeconds();
    return lastUpdate + (updateInterval * 1000) < System.currentTimeMillis();
  }

  public synchronized void updateView(OView view, ODatabaseDocumentInternal db) {
    lastUpdateTimestampForView.put(view.getName(), System.currentTimeMillis());

    int cluster = db.addCluster(getNextClusterNameFor(view, db));

    String viewName = view.getName();
    String query = view.getQuery();
    String originRidField = view.getOriginRidField();
    String clusterName = db.getClusterNameById(cluster);

    List<OIndex> indexes = createNewIndexesForView(view, cluster, db);

    OScenarioThreadLocal.executeAsDistributed(new Callable<Object>() {
      @Override
      public Object call() {

        OResultSet rs = db.query(query);
        while (rs.hasNext()) {
          OResult item = rs.next();
          addItemToView(item, db, originRidField, viewName, clusterName, indexes);
        }

        return null;
      }

    });

    view = db.getMetadata().getSchema().getView(view.getName());
    if (view == null) {
      //the view was dropped in the meantime
      db.dropCluster(clusterName);
      indexes.forEach(x -> x.delete());
      return;
    }
    lockView(view);
    view.addClusterId(cluster);
    for (int i : view.getClusterIds()) {
      if (i != cluster) {
        clustersToDrop.add(i);
        viewCluserVisitors.put(i, new AtomicInteger(0));
        oldClustersPerViews.put(i, view.getName());
        view.removeClusterId(i);
      }
    }

    final OViewImpl viewImpl = ((OViewImpl) view);
    viewImpl.getInactiveIndexes().forEach(idx -> {
      indexesToDrop.add(idx);
      viewIndexVisitors.put(idx, new AtomicInteger(0));
      oldIndexesPerViews.put(idx, viewName);
    });
    viewImpl.inactivateIndexes();
    viewImpl.addActiveIndexes(indexes.stream().map(x -> x.getName()).collect(Collectors.toList()));

    unlockView(view);
    cleanUnusedViewIndexes(db);
    cleanUnusedViewClusters(db);

  }

  private void addItemToView(OResult item, ODatabaseDocument db, String originRidField, String viewName, String clusterName,
      List<OIndex> indexes) {
    OElement newRow = copyElement(item, db);
    if (originRidField != null) {
      newRow.setProperty(originRidField, item.getIdentity().orElse(item.getProperty("@rid")));
      newRow.setProperty("@view", viewName);
    }
    db.save(newRow, clusterName);

    indexes.forEach(idx -> idx.put(indexedKeyFor(idx, newRow), newRow));
  }

  private Object indexedKeyFor(OIndex idx, OElement newRow) {
    List<String> fieldsToIndex = idx.getDefinition().getFieldsToIndex();
    if (fieldsToIndex.size() == 1) {
      return idx.getDefinition().createValue((Object) newRow.getProperty(fieldsToIndex.get(0)));
    }
    Object[] vals = new Object[fieldsToIndex.size()];
    for (int i = 0; i < fieldsToIndex.size(); i++) {
      vals[i] = newRow.getProperty(fieldsToIndex.get(i));
    }
    return idx.getDefinition().createValue(vals);
  }

  private List<OIndex> createNewIndexesForView(OView view, int cluster, ODatabaseDocumentInternal db) {
    try {
      List<OIndex> result = new ArrayList<>();
      OIndexManagerAbstract idxMgr = db.getMetadata().getIndexManagerInternal();
      for (OViewConfig.OViewIndexConfig cfg : view.getRequiredIndexesInfo()) {
        OIndexDefinition definition = createIndexDefinition(view.getName(), cfg.getProperties());
        String indexName = view.getName() + "_" + UUID.randomUUID().toString().replaceAll("-", "_");
        String type = cfg.getType();
        String engine = cfg.getEngine();
        OIndex idx = idxMgr.createIndex(db, indexName, type, definition, new int[] { cluster }, null, null, engine);
        result.add(idx);
      }
      return result;
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  private OIndexDefinition createIndexDefinition(String viewName, List<OPair<String, OType>> requiredIndexesInfo) {
    if (requiredIndexesInfo.size() == 1) {
      return new OPropertyIndexDefinition(viewName, requiredIndexesInfo.get(0).getKey(), requiredIndexesInfo.get(0).getValue());
    }
    OCompositeIndexDefinition result = new OCompositeIndexDefinition(viewName);
    for (OPair<String, OType> pair : requiredIndexesInfo) {
      result.addIndex(new OPropertyIndexDefinition(viewName, pair.getKey(), pair.getValue()));
    }
    return result;
  }

  private synchronized void unlockView(OView view) {
    //TODO
  }

  private void lockView(OView view) {
    //TODO
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
    orientDB.executeNoAuthorization(dbName, (databaseSession) -> {
      if (!buildOnThisNode(databaseSession, name)) {
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

  public synchronized void startUsingViewCluster(Integer cluster) {
    AtomicInteger item = viewCluserVisitors.get(cluster);
    if (item == null) {
      item = new AtomicInteger(0);
      viewCluserVisitors.put(cluster, item);
    }
    item.incrementAndGet();
  }

  public synchronized void endUsingViewCluster(Integer cluster) {
    AtomicInteger item = viewCluserVisitors.get(cluster);
    if (item == null) {
      return;
    }
    item.decrementAndGet();
  }

  public void recordAdded(OImmutableClass clazz, ODocument doc, ODatabaseDocumentEmbedded oDatabaseDocumentEmbedded) {
    lastChangePerClass.put(clazz.getName().toLowerCase(Locale.ENGLISH), System.currentTimeMillis());
  }

  public void recordUpdated(OImmutableClass clazz, ODocument doc, ODatabaseDocumentEmbedded oDatabaseDocumentEmbedded) {
    lastChangePerClass.put(clazz.getName().toLowerCase(Locale.ENGLISH), System.currentTimeMillis());
  }

  public void recordDeleted(OImmutableClass clazz, ODocument doc, ODatabaseDocumentEmbedded oDatabaseDocumentEmbedded) {
    lastChangePerClass.put(clazz.getName().toLowerCase(Locale.ENGLISH), System.currentTimeMillis());
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
        addItemToView(data, db, view.getOriginRidField(), view.getName(), db.getClusterNameById(cluster),
            new ArrayList<>(view.getIndexes()));
      }
    }

    @Override
    public void onUpdate(ODatabaseDocument db, OResult before, OResult after) {
      OView view = db.getMetadata().getSchema().getView(viewName);
      if (view != null && view.getOriginRidField() != null) {
        try (OResultSet rs = db
            .query("SELECT FROM " + viewName + " WHERE " + view.getOriginRidField() + " = ?", (Object) after.getProperty("@rid"))) {
          while (rs.hasNext()) {
            OResult row = rs.next();
            row.getElement().ifPresent(elem -> updateViewRow(elem, after, view, (ODatabaseDocumentInternal) db));
          }
        }
      }
    }

    private void updateViewRow(OElement viewRow, OResult origin, OView view, ODatabaseDocumentInternal db) {
      OStatement stm = OStatementCache.get(view.getQuery(), db);
      if (stm instanceof OSelectStatement) {
        OProjection projection = ((OSelectStatement) stm).getProjection();
        if (projection == null || (projection.getItems().size() == 0 && projection.getItems().get(0).isAll())) {
          for (String s : origin.getPropertyNames()) {
            if ("@rid".equalsIgnoreCase(s) || "@class".equalsIgnoreCase(s) || "@version".equalsIgnoreCase(s)) {
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
        try (OResultSet rs = db
            .query("SELECT FROM " + viewName + " WHERE " + view.getOriginRidField() + " = ?", (Object) data.getProperty("@rid"))) {
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
    public void onEnd(ODatabaseDocument database) {

    }
  }
}