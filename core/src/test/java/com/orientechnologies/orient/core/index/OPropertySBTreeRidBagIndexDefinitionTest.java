package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 1/30/14
 */
public class OPropertySBTreeRidBagIndexDefinitionTest extends OPropertyRidBagAbstractIndexDefinitionTest {
	private int topThreshold;
	private int bottomThreshold;

	protected ODatabaseDocumentTx database;

	@BeforeClass
	public void beforeClass() {
		final String buildDirectory = System.getProperty("buildDirectory", ".");
		final String url = "plocal:" + buildDirectory + "/test-db/" + this.getClass().getSimpleName();
		database = new ODatabaseDocumentTx(url);
		if (database.exists()) {
			database.open("admin", "admin");
			database.drop();
		}

		database.create();
		database.close();
	}

	@BeforeMethod
	public void beforeMethod() {
		super.beforeMethod();

		topThreshold = OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger();
		bottomThreshold = OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.getValueAsInteger();

		OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(-1);
		OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(-1);

		database.open("admin", "admin");
	}

	@AfterMethod
	public void afterMethod() {
		database.close();

		OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(topThreshold);
		OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(bottomThreshold);
	}

}
