package com.orientechnologies.orient.core.intent;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;

public class OIntentMassiveInsert implements OIntent {

	public void activate(final ODatabase iDatabase, final Object... iArgs) {
		if (iDatabase instanceof ODatabaseObject)
			((ODatabaseObject) iDatabase).setRetainObjects(false);
	}
}
