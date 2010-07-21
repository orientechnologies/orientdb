package com.orientechnologies.orient.core.intent;

import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.db.raw.ODatabaseRaw;

public class OIntentMassiveInsert implements OIntent {
	private boolean	previousRetainObjects;

	public void begin(final ODatabaseRaw iDatabase, final Object... iArgs) {
		final ODatabaseComplex<?> ownerDb = iDatabase.getDatabaseOwner();

		if (ownerDb instanceof ODatabaseObject) {
			previousRetainObjects = ((ODatabaseObject) ownerDb).isRetainObjects();
			((ODatabaseObject) ownerDb).setRetainObjects(false);
		}
	}

	public void end(final ODatabaseRaw iDatabase) {
		final ODatabaseComplex<?> ownerDb = iDatabase.getDatabaseOwner();

		if (ownerDb instanceof ODatabaseObject) {
			previousRetainObjects = ((ODatabaseObject) ownerDb).isRetainObjects();
			((ODatabaseObject) ownerDb).setRetainObjects(previousRetainObjects);
		}
	}
}
