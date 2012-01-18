package com.orientechnologies.orient.console;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseListener;

public class OConsoleDatabaseListener implements ODatabaseListener {
	OConsoleDatabaseApp	console;

	public OConsoleDatabaseListener(OConsoleDatabaseApp console) {
		this.console = console;
	}

	public void onCreate(ODatabase iDatabase) {
	}

	public void onDelete(ODatabase iDatabase) {
	}

	public void onOpen(ODatabase iDatabase) {
	}

	public void onBeforeTxBegin(ODatabase iDatabase) {
	}

	public void onBeforeTxRollback(ODatabase iDatabase) {
	}

	public void onAfterTxRollback(ODatabase iDatabase) {
	}

	public void onBeforeTxCommit(ODatabase iDatabase) {
	}

	public void onAfterTxCommit(ODatabase iDatabase) {
	}

	public void onClose(ODatabase iDatabase) {
	}

	public boolean onCorruptionRepairDatabase(ODatabase iDatabase, final String iProblem, String iWhatWillbeFixed) {
		final String answer = console.ask("\nDatabase seems corrupted:\n> " + iProblem + "\nAuto-repair will execute this action:\n> "
				+ iWhatWillbeFixed + "\n\nDo you want to repair it (Y/n)? ");
		return answer.length() == 0 || answer.equalsIgnoreCase("Y") || answer.equalsIgnoreCase("Yes");
	}
}
