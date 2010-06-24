package com.orientechnologies.orient.core.intent;

import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.db.raw.ODatabaseRaw;

public class OIntentMassiveInsert implements OIntent {

	public void activate(final ODatabaseRaw iDatabase, final Object... iArgs) {
		final ODatabaseComplex<?> ownerDb = iDatabase.getDatabaseOwner();

		if (ownerDb instanceof ODatabaseObject)
			((ODatabaseObject) ownerDb).setRetainObjects(false);
	}
}
