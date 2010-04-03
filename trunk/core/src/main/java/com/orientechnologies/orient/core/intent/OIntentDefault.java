package com.orientechnologies.orient.core.intent;

import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.db.raw.ODatabaseRaw;

public class OIntentDefault implements OIntent {

	public void activate(ODatabaseRaw iDatabase, final Object... iArgs) {
		iDatabase.setUseCache(true);

		ODatabaseComplex<?> ownerDb = iDatabase.getDatabaseOwner();

		while (ownerDb.getDatabaseOwner() != null && ownerDb.getDatabaseOwner() != ownerDb)
			ownerDb = ownerDb.getDatabaseOwner();

		if (iDatabase instanceof ODatabaseObject)
			((ODatabaseObject) iDatabase).setRetainObjects(true);
	}
}
