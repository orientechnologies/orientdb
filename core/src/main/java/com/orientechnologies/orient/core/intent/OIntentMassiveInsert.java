package com.orientechnologies.orient.core.intent;

import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.ODatabasePojoAbstract;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.db.raw.ODatabaseRaw;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;

public class OIntentMassiveInsert implements OIntent {
	private boolean	previousUseCache;
	private boolean	previousRetainRecords;
	private boolean	previousRetainObjects;

	public void begin(final ODatabaseRaw iDatabase, final Object... iArgs) {
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
		iDatabase.setUseCache(previousUseCache);

		final ODatabaseComplex<?> ownerDb = iDatabase.getDatabaseOwner();

		if (ownerDb instanceof ODatabaseRecord<?>)
			((ODatabaseRecord<?>) ownerDb).setRetainRecords(previousRetainRecords);

		if (ownerDb instanceof ODatabaseObject)
			((ODatabasePojoAbstract<?, ?>) ownerDb).setRetainObjects(previousRetainObjects);
	}
}
