/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.command;

import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.record.OIdentifiable;

/**
 * Text based Command Request abstract class.
 * 
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("serial")
public abstract class OCommandRequestAbstract implements OCommandRequestInternal {
	protected OCommandResultListener	resultListener;
	protected OProgressListener				progressListener;
	protected int											limit	= -1;
	protected Map<Object, Object>			parameters;

	protected OCommandRequestAbstract() {
	}

	public OCommandResultListener getResultListener() {
		return resultListener;
	}

	public void setResultListener(OCommandResultListener iListener) {
		resultListener = iListener;
	}

	public Map<Object, Object> getParameters() {
		return parameters;
	}

	@SuppressWarnings("unchecked")
	protected void setParameters(final Object... iArgs) {
		if (iArgs.length > 0) {
			if (iArgs.length == 1 && iArgs[0] instanceof Map) {
				parameters = (Map<Object, Object>) iArgs[0];
			} else {
				parameters = new HashMap<Object, Object>();
				for (int i = 0; i < iArgs.length; ++i) {
					Object par = iArgs[i];

					if (par instanceof OIdentifiable && ((OIdentifiable) par).getIdentity().isValid())
						// USE THE RID ONLY
						par = ((OIdentifiable) par).getIdentity();

					parameters.put(i, par);
				}
			}
		}
	}

	public OProgressListener getProgressListener() {
		return progressListener;
	}

	public OCommandRequestAbstract setProgressListener(OProgressListener progressListener) {
		this.progressListener = progressListener;
		return this;
	}

	public void reset() {
	}

	public int getLimit() {
		return limit;
	}

	public OCommandRequestAbstract setLimit(final int limit) {
		this.limit = limit;
		return this;
	}

}
