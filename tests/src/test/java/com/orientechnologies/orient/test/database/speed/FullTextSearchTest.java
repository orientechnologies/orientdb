package com.orientechnologies.orient.test.database.speed;

import java.util.List;
import java.util.Map.Entry;

import org.testng.annotations.Test;

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OFullTextIndex;
import com.orientechnologies.orient.core.index.OFullTextIndexManager;
import com.orientechnologies.orient.core.intent.OIntentMassiveRead;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;

@Test(sequential = true)
public class FullTextSearchTest {
	private static final int	DOCUMENTS	= 1000;

	public static void main(String[] iArgs) throws InstantiationException, IllegalAccessException {
		OProfiler.getInstance().startRecording();

		final ODatabaseDocumentTx database = new ODatabaseDocumentTx(System.getProperty("url")).open("admin", "admin");

		final OFullTextIndexManager indexMgr = new OFullTextIndexManager(database);
		OFullTextIndex index = indexMgr.getIndex("test");

		database.declareIntent(new OIntentMassiveRead());
		database.begin(TXTYPE.NOTX);

		long time = System.currentTimeMillis();

		List<ORecordId> recs = index.get("wife");

		if (recs == null)
			System.out.println("\nERROR: Not found!");
		else
			System.out.println("\nSearch for keyword 'wife' in " + ((System.currentTimeMillis() - time) / 1000f) + " sec.");

		for (Entry<String, List<ORecordId>> entry : index) {
			System.out.println("- " + entry.getKey() + ":" + entry.getValue().size());
		}

		database.close();
	}
}
