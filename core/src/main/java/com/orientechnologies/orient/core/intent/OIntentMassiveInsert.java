package com.orientechnologies.orient.core.intent;

import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.ODatabasePojoAbstract;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.db.raw.ODatabaseRaw;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;

public class OIntentMassiveInsert implements OIntent {
	private int			previousDatabaseCacheSize;
	private int			previousStorageCacheSize;
	private boolean	previousUseCache;
	private boolean	previousRetainRecords;
	private boolean	previousRetainObjects;

	public void begin(final ODatabaseRaw iDatabase, final Object... iArgs) {
		previousUseCache = iDatabase.isUseCache();
		iDatabase.setUseCache(false);
		previousDatabaseCacheSize = iDatabase.getCache().getMaxSize();
		iDatabase.getCache().setMaxSize(0);
		previousStorageCacheSize = iDatabase.getStorage().getCache().getMaxSize();
		iDatabase.getStorage().getCache().setMaxSize(0);

		ODatabaseComplex<?> ownerDb = iDatabase.getDatabaseOwner();

		if (ownerDb instanceof ODatabaseRecord) {
			previousRetainRecords = ((ODatabaseRecord) ownerDb).isRetainRecords();
			((ODatabaseRecord) ownerDb).setRetainRecords(false);
		}

		while (ownerDb.getDatabaseOwner() != ownerDb)
			ownerDb = ownerDb.getDatabaseOwner();

		if (ownerDb instanceof ODatabasePojoAbstract) {
			previousRetainObjects = ((ODatabasePojoAbstract<?>) ownerDb).isRetainObjects();
			((ODatabasePojoAbstract<?>) ownerDb).setRetainObjects(false);
		}
	}

	public void end(final ODatabaseRaw iDatabase) {
		iDatabase.setUseCache(previousUseCache);
		iDatabase.getCache().setMaxSize(previousDatabaseCacheSize);
		iDatabase.getStorage().getCache().setMaxSize(previousStorageCacheSize);

		final ODatabaseComplex<?> ownerDb = iDatabase.getDatabaseOwner();

		if (ownerDb instanceof ODatabaseRecord)
			((ODatabaseRecord) ownerDb).setRetainRecords(previousRetainRecords);

		if (ownerDb instanceof ODatabaseObject)
			((ODatabasePojoAbstract<?>) ownerDb).setRetainObjects(previousRetainObjects);
	}
}
