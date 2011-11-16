package com.orientechnologies.orient.core.storage.impl.local;

public class TestSimulateError {

	public static TestSimulateError	onDataLocalWriteRecord;

	public static boolean onDataLocalWriteRecord(ODataLocal iODataLocal, long[] iFilePosition, int iClusterSegment,
			long iClusterPosition, byte[] iContent) {
		if (onDataLocalWriteRecord != null)
			return onDataLocalWriteRecord.checkDataLocalWriteRecord(iODataLocal, iFilePosition, iClusterSegment, iClusterPosition,
					iContent);
		return true;
	}

	public boolean checkDataLocalWriteRecord(ODataLocal iODataLocal, long[] iFilePosition, int iClusterSegment,
			long iClusterPosition, byte[] iContent) {
		return true;
	}
}
