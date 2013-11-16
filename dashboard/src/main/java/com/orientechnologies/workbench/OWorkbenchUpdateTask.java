package com.orientechnologies.workbench;

import java.io.File;
import java.util.TimerTask;

public class OWorkbenchUpdateTask extends TimerTask {

	private OWorkbenchPlugin	handler;

	public OWorkbenchUpdateTask(final OWorkbenchPlugin iHandler) {
		this.handler = iHandler;
	}

	@Override
	public void run() {
		String dbDir = handler.getServerInstance().getDatabaseDirectory();
		File dbDirFile = new File(dbDir);
		String workbenchDir = dbDirFile.getParent();
		File f = new File(workbenchDir + "/download");
		if (f.exists()) {
			for (File file : f.listFiles()) {
				if (file.getName().startsWith("agent")) {
					File dest = new File(workbenchDir+"/agents/" + file.getName());
					file.renameTo(dest);
				} else if (file.getName().startsWith("orientdb-workbench")) {
					File dest = new File(workbenchDir+"/plugins/" + file.getName());
					file.renameTo(dest);
				}
			}
		}

	}
}
