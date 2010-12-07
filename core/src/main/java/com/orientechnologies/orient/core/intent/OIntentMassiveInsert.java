package com.orientechnologies.orient.core.intent;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.ODatabasePojoAbstract;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.db.raw.ODatabaseRaw;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;

public class OIntentMassiveInsert implements OIntent {
	private boolean	previousUseCache;
	private boolean	previousRetainRecords;
	private boolean	previousRetainObjects;
	private int			treeMapLazyUpdates;
	private int			treeMapOptimizeThreshold;

	public void begin(final ODatabaseRaw iDatabase, final Object... iArgs) {
		treeMapLazyUpdates = OGlobalConfiguration.MVRBTREE_LAZY_UPDATES.getValueAsInteger();
		OGlobalConfiguration.MVRBTREE_LAZY_UPDATES.setValue(10000);

		treeMapOptimizeThreshold = OGlobalConfiguration.MVRBTREE_OPTIMIZE_THRESHOLD.getValueAsInteger();
		OGlobalConfiguration.MVRBTREE_OPTIMIZE_THRESHOLD.setValue(1000000);

		previousUseCache = iDatabase.isUseCache();
		iDatabase.setUseCache(false);

		ODatabaseComplex<?> ownerDb = iDatabase.getDatabaseOwner();

		if (ownerDb instanceof ODatabaseRecord<?>) {
			previousRetainRecords = ((ODatabaseRecord<?>) ownerDb).isRetainRecords();
			((ODatabaseRecord<?>) ownerDb).setRetainRecords(false);
		}

		while (ownerDb.getDatabaseOwner() != ownerDb)
			ownerDb = ownerDb.getDatabaseOwner();

		if (ownerDb instanceof ODatabasePojoAbstract) {
			previousRetainObjects = ((ODatabasePojoAbstract<?, ?>) ownerDb).isRetainObjects();
			((ODatabasePojoAbstract<?, ?>) ownerDb).setRetainObjects(false);
		}
	}

	public void end(final ODatabaseRaw iDatabase) {
		OGlobalConfiguration.MVRBTREE_LAZY_UPDATES.setValue(treeMapLazyUpdates);
		OGlobalConfiguration.MVRBTREE_OPTIMIZE_THRESHOLD.setValue(treeMapOptimizeThreshold);

		iDatabase.setUseCache(previousUseCache);

		final ODatabaseComplex<?> ownerDb = iDatabase.getDatabaseOwner();

		if (ownerDb instanceof ODatabaseRecord<?>)
			((ODatabaseRecord<?>) ownerDb).setRetainRecords(previousRetainRecords);

		if (ownerDb instanceof ODatabaseObject)
			((ODatabasePojoAbstract<?, ?>) ownerDb).setRetainObjects(previousRetainObjects);
	}
}
