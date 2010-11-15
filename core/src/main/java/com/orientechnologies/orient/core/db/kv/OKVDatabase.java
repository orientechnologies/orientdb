package com.orientechnologies.orient.core.db.kv;

import java.io.IOException;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordAbstract;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerString;
import com.orientechnologies.orient.core.storage.impl.local.ODictionaryLocal;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeDatabase;

public class OKVDatabase extends ODatabaseDocumentTx {
	public OKVDatabase(final String iURL) {
		super(iURL);
	}

	public OMVRBTreeDatabase<String, String> getBucket(final ODatabaseRecordAbstract<ORecordBytes> iDb, final String iBucket)
			throws IOException {
		ORecordBytes rec = iDb.getDictionary().get(iBucket);

		OMVRBTreeDatabase<String, String> bucketTree = null;

		if (rec != null) {
			bucketTree = new OMVRBTreeDatabase<String, String>(iDb, rec.getIdentity());
			bucketTree.load();
		}

		if (bucketTree == null) {
			// CREATE THE BUCKET
			bucketTree = new OMVRBTreeDatabase<String, String>(iDb, ODictionaryLocal.DICTIONARY_DEF_CLUSTER_NAME,
					OStreamSerializerString.INSTANCE, OStreamSerializerString.INSTANCE);
			bucketTree.save();

			iDb.getDictionary().put(iBucket, bucketTree.getRecord());
		}
		return bucketTree;
	}
}
