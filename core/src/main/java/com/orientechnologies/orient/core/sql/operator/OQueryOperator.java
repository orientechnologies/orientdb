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
package com.orientechnologies.orient.core.sql.operator;

import java.util.List;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.operator.math.OQueryOperatorDivide;
import com.orientechnologies.orient.core.sql.operator.math.OQueryOperatorMinus;
import com.orientechnologies.orient.core.sql.operator.math.OQueryOperatorMod;
import com.orientechnologies.orient.core.sql.operator.math.OQueryOperatorMultiply;
import com.orientechnologies.orient.core.sql.operator.math.OQueryOperatorPlus;

/**
 * Query Operators. Remember to handle the operator in OQueryItemCondition.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class OQueryOperator {
    
    public static enum ORDER{
            /**
             * Used when order compared to other operator can not be evaluated
             * or has no consequences.
             */
            UNKNOWNED,
            /**
             * Used when this operator must be before the other one
             */
            BEFORE,
            /**
             * Used when this operator must be after the other one
             */
            AFTER,
            /**
             * Used when this operator is equal the other one
             */
            EQUAL
        }
                
        /**
         * Default operator order. can be used by additional operator
         * to locate themself relatively to default ones.
         * 
         * WARNING: ORDER IS IMPORTANT TO AVOID SUB-STRING LIKE "IS" and AND "INSTANCEOF": INSTANCEOF MUST BE PLACED BEFORE! AND ALSO FOR
         * PERFORMANCE (MOST USED BEFORE)
         */
        protected static final Class[] DEFAULT_OPERATORS_ORDER = {
            OQueryOperatorEquals.class,
            OQueryOperatorAnd.class, 
            OQueryOperatorOr.class, 
            OQueryOperatorNotEquals.class, 
            OQueryOperatorNot.class,
            OQueryOperatorMinorEquals.class, 
            OQueryOperatorMinor.class, 
            OQueryOperatorMajorEquals.class, 
            OQueryOperatorContainsAll.class,
            OQueryOperatorMajor.class, 
            OQueryOperatorLike.class, 
            OQueryOperatorMatches.class, 
            OQueryOperatorInstanceof.class,
            OQueryOperatorIs.class, 
            OQueryOperatorIn.class, 
            OQueryOperatorContainsKey.class, 
            OQueryOperatorContainsValue.class,
            OQueryOperatorContainsText.class, 
            OQueryOperatorContains.class,
            OQueryOperatorTraverse.class, 
            OQueryOperatorBetween.class, 
            OQueryOperatorPlus.class, 
            OQueryOperatorMinus.class,
            OQueryOperatorMultiply.class, 
            OQueryOperatorDivide.class, 
            OQueryOperatorMod.class};
    
	public final String		keyword;
	public final int			precedence;
	public final int			expectedRightWords;
	public final boolean	unary;

	protected OQueryOperator(final String iKeyword, final int iPrecedence, final boolean iUnary) {
		keyword = iKeyword;
		precedence = iPrecedence;
		unary = iUnary;
		expectedRightWords = 1;
	}

	protected OQueryOperator(final String iKeyword, final int iPrecedence, final boolean iUnary, final int iExpectedRightWords) {
		keyword = iKeyword;
		precedence = iPrecedence;
		unary = iUnary;
		expectedRightWords = iExpectedRightWords;
	}

	public abstract Object evaluateRecord(final OIdentifiable iRecord, final OSQLFilterCondition iCondition, final Object iLeft,
			final Object iRight, OCommandContext iContext);

	public abstract OIndexReuseType getIndexReuseType(Object iLeft, Object iRight);

	@Override
	public String toString() {
		return keyword;
	}

	/**
	 * Default State-less implementation: does not save parameters and just return itself
	 * 
	 * @param iParams
	 * @return
	 */
	public OQueryOperator configure(final List<String> iParams) {
		return this;
	}

	public String getSyntax() {
		return "<left> " + keyword + " <right>";
	}

	public abstract ORID getBeginRidRange(final Object iLeft, final Object iRight);

	public abstract ORID getEndRidRange(final Object iLeft, final Object iRight);

	public boolean isUnary() {
		return unary;
	}
        
        /**
         * Check priority of this operator compare to given operator.
         * @param other
         * @return ORDER place of this operator compared to given operator
         */
        public ORDER compare(OQueryOperator other){
            final Class thisClass = this.getClass();
            final Class otherClass = other.getClass();
            
            int thisPosition = -1;
            int otherPosition = -1;
            for(int i=0; i<DEFAULT_OPERATORS_ORDER.length; i++){
                //subclass of default operators inherit their parent ordering
                final Class clazz = DEFAULT_OPERATORS_ORDER[i];
                if(clazz.isAssignableFrom(thisClass)){
                    thisPosition = i;
                }
                if(clazz.isAssignableFrom(otherClass)){
                    otherPosition = i;
                }
            }
            
            if(thisPosition == -1 || otherPosition == -1){
                //can not decide which comes first
                return ORDER.UNKNOWNED;
            }
            
            if(thisPosition > otherPosition){
                return ORDER.AFTER;
            }else if(thisPosition < otherPosition){
                return ORDER.BEFORE;
            }
            
            return ORDER.EQUAL;                        
        }
        
}
