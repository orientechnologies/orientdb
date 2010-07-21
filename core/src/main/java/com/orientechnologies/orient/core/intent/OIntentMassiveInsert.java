package com.orientechnologies.orient.core.intent;

import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.db.raw.ODatabaseRaw;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;

public class OIntentMassiveInsert implements OIntent {
	private boolean	previousRetainRecords;
	private boolean	previousRetainObjects;

	public void begin(final ODatabaseRaw iDatabase, final Object... iArgs) {
		final ODatabaseComplex<?> ownerDb = iDatabase.getDatabaseOwner();

		if (ownerDb instanceof ODatabaseRecord<?>) {
			previousRetainRecords = ((ODatabaseRecord<?>) ownerDb).isRetainRecords();
			((ODatabaseRecord<?>) ownerDb).setRetainRecords(false);
		}

		if (ownerDb instanceof ODatabaseObject) {
			previousRetainObjects = ((ODatabaseObject) ownerDb).isRetainObjects();
			((ODatabaseObject) ownerDb).setRetainObjects(false);
		}
	}

	public void end(final ODatabaseRaw iDatabase) {
		final ODatabaseComplex<?> ownerDb = iDatabase.getDatabaseOwner();

		if (ownerDb instanceof ODatabaseRecord<?>)
			((ODatabaseRecord<?>) ownerDb).setRetainRecords(previousRetainRecords);

		if (ownerDb instanceof ODatabaseObject)
			((ODatabaseObject) ownerDb).setRetainObjects(previousRetainObjects);
	}
}
