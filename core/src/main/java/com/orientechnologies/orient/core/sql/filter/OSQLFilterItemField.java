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
package com.orientechnologies.orient.core.sql.filter;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.command.OCommandToParse;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;

import java.util.List;

/**
 * Represent an object field as value in the query condition.
 * 
 * @author Luca Garulli
 * 
 */
public class OSQLFilterItemField extends OSQLFilterItemAbstract {
	protected String	name;

	public OSQLFilterItemField(final OCommandToParse iQueryToParse, final String iName) {
		super(iQueryToParse, iName);
	}

	public Object getValue(final OIdentifiable iRecord) {
		if (iRecord == null)
			throw new OCommandExecutionException("expression item '" + name + "' cannot be resolved");

		return transformValue(iRecord, ODocumentHelper.getFieldValue((ODocument) iRecord, name));
	}

	public String getRoot() {
		return name;
	}

	public void setRoot(final OCommandToParse iQueryToParse, final String iRoot) {
		this.name = iRoot;
	}

    /**
     * Check whether or not this filter item is chain of fields (e.g. "field1.field2.field3").
     * Return true if filter item contains only field projections operators, if field item
     * contains any other projection operator the method returns false.
     * When filter item does not contains any chain operator, it is also field chain consist of one field.
     * @return whether or not this filter item can be represented as chain of fields.
     */
    public boolean isFieldChain() {
        if (operationsChain == null) {
            return true;
        }

        for (OPair<Integer, List<String>> pair : operationsChain) {
            if (!pair.getKey().equals(OSQLFilterFieldOperator.FIELD.id)) {
                return false;
            }
        }

        return true;
    }

    /**
     * @return {@code FieldChain} representation of this filter item.
     * @throws IllegalStateException if this filter item can't be represented as {@code FieldChain}.
     */
    public FieldChain getFieldChain() {
        if (!isFieldChain()) {
            throw new IllegalStateException("Filter item field contains not only field operators");
        }

        return new FieldChain();
    }

    public class FieldChain {
        private FieldChain() {
        }

        public String getItemName(int fieldIndex) {
            if (fieldIndex == 0) {
                return name;
            } else {
                return operationsChain.get(fieldIndex - 1).getValue().get(0);
            }
        }

        public int getItemCount() {
            if (operationsChain == null) {
                return 1;
            } else {
                return operationsChain.size() + 1;
            }
        }

        /**
         * Field chain is considered as long chain if it contains more than one item.
         *
         * @return true if this chain is long and false in another case.
         */
        public boolean isLong() {
            return operationsChain != null && operationsChain.size() > 0;
        }
    }
}
