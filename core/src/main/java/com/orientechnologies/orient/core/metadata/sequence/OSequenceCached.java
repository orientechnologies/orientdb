/*
 *
 *  *  Copyright 2014 OrientDB LTD (info(at)orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.orient.core.metadata.sequence;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.concurrent.Callable;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 3/3/2015
 */
public class OSequenceCached extends OSequence {
  private static final String FIELD_CACHE = "cache";
  private              long   cacheStart;
  private              long   cacheEnd;  
  private boolean firstCache;
  private int increment;
  private Long limitValue = null;
  private Long startValue;
  private SequenceOrderType orderType;
  private boolean recyclable;
  private String name = null;

  public OSequenceCached() {
    this(null, null);
  }

  public OSequenceCached(final ODocument iDocument) {
    this(iDocument, null);
  }

  public OSequenceCached(final ODocument iDocument, OSequence.CreateParams params) {
    super(iDocument, params);    
    if (iDocument != null){
      firstCache = true;
      cacheStart = cacheEnd = getValue(iDocument);
    }
  }

  @Override
  public synchronized boolean updateParams(OSequence.CreateParams params) {
    boolean any = super.updateParams(params);
    if (params.cacheSize != null && this.getCacheSize() != params.cacheSize) {
      this.setCacheSize(params.cacheSize);
      any = true;
    }
    return any;
  }

  @Override
  protected void initSequence(OSequence.CreateParams params) {    
    super.initSequence(params);
    setCacheSize(params.cacheSize);
    cacheStart = cacheEnd = 0L;    
    allocateCache(getCacheSize(), getDatabase());
  }
  
  private void doRecycle(ODatabaseDocumentInternal finalDb){
    if (recyclable){
      setValue(getStart());
      allocateCache(getCacheSize(), finalDb);
    }
    else{
      throw new OSequenceLimitReachedException("Limit reached");
    }
  }
  
  private void reloadCrucialValues(){
    increment = getIncrement();
    limitValue = getLimitValue();
    orderType = getOrderType();    
    recyclable = getRecyclable();
    startValue = getStart();
    if (name == null){
      name = getName();
    }
  }
  
  @Override
  public long next() throws OSequenceLimitReachedException{
    ODatabaseDocumentInternal mainDb = getDatabase();
    boolean tx = mainDb.getTransaction().isActive();
    try {
      ODatabaseDocumentInternal db = mainDb;
      if (tx) {
        db = mainDb.copy();
        db.activateOnCurrentThread();
      }
      try {
        ODatabaseDocumentInternal finalDb = db;
        return callRetry(new Callable<Long>() {
          @Override
          public Long call() throws Exception {
            synchronized (OSequenceCached.this) {              
              
              boolean detectedCrucialValueChange = false;
              if (getCrucilaValueChanged()){
                reloadCrucialValues();
                detectedCrucialValueChange = true;                
              }
              if (orderType == SequenceOrderType.ORDER_POSITIVE){
                if (cacheStart + increment > cacheEnd && !(limitValue != null && cacheStart + increment > limitValue)) {                  
                  boolean cachedbefore = !firstCache;
                  allocateCache(getCacheSize(), finalDb);
                  if (!cachedbefore){
                    if (limitValue != null && cacheStart + increment > limitValue){
                      doRecycle(finalDb);
                    }
                    else{
                      cacheStart = cacheStart + increment;
                    }
                  }
                }
                else if (limitValue != null && cacheStart + increment > limitValue){
                  doRecycle(finalDb);
                }
                else{
                  cacheStart = cacheStart + increment;
                }
              }
              else{
                if (cacheStart - increment < cacheEnd && !(limitValue != null && cacheStart - increment < limitValue)) {
                  boolean cachedbefore = !firstCache;
                  allocateCache(getCacheSize(), finalDb);
                  if (!cachedbefore){
                    if (limitValue != null && cacheStart - increment < limitValue){
                      doRecycle(finalDb);
                    }
                    else{
                      cacheStart = cacheStart - increment;
                    }
                  }
                }
                else if (limitValue != null && cacheStart - increment < limitValue){
                  doRecycle(finalDb);
                }
                else{
                  cacheStart = cacheStart - increment;
                }
              }
              
              if (detectedCrucialValueChange){
                setCrucialValueChanged(false);
              }
              
              if (limitValue != null && !recyclable){
                float tillEnd = Math.abs(limitValue - cacheStart) / (float)increment;
                float delta = Math.abs(limitValue - startValue) / (float)increment;
                //warning on 1%
                if ((float)tillEnd <= ((float)delta / 100.f) || tillEnd <= 1){
                  String warningMessage = "Non-recyclable sequence: " + name + " reaching limt, current value: " + cacheStart + " limit value: " + limitValue + " with step: " + increment;
                  OLogManager.instance().warn(this, warningMessage);
                }
              }
              
              return cacheStart;
            }
          }
        }, "next");
      } finally {
        if (tx) {
          db.close();
        }
      }
    } finally {
      if (tx) {
        mainDb.activateOnCurrentThread();
      }
    }
  }

  @Override
  public synchronized long current() {
    return this.cacheStart;
  }

  @Override
  public long reset() {
    ODatabaseDocumentInternal mainDb = getDatabase();
    boolean tx = mainDb.getTransaction().isActive();
    try {
      ODatabaseDocumentInternal db = mainDb;
      if (tx) {
        db = mainDb.copy();
        db.activateOnCurrentThread();
      }
      try {
        ODatabaseDocumentInternal finalDb = db;
        return callRetry(new Callable<Long>() {
          @Override
          public Long call() throws Exception {
            synchronized (OSequenceCached.this) {
              long newValue = getStart();
              setValue(newValue);
              save(finalDb);
              firstCache = true;
              allocateCache(getCacheSize(), finalDb);
              return newValue;
            }
          }
        }, "reset");
      } finally {
        if (tx) {
          db.close();
        }
      }
    } finally {
      if (tx) {
        mainDb.activateOnCurrentThread();
      }
    }
  }

  @Override
  public SEQUENCE_TYPE getSequenceType() {
    return SEQUENCE_TYPE.CACHED;
  }

  public int getCacheSize() {
    return getDocument().field(FIELD_CACHE, OType.INTEGER);
  }

  public void setCacheSize(int cacheSize) {
    getDocument().field(FIELD_CACHE, cacheSize);
  }

  private void allocateCache(int cacheSize, ODatabaseDocumentInternal db) {
    if (getCrucilaValueChanged()){
      reloadCrucialValues();
      setCrucialValueChanged(false);
    }
    SequenceOrderType orederType = getOrderType();
    long value = getValue();
    long newValue;
    if (orederType == SequenceOrderType.ORDER_POSITIVE){
      newValue = value + (getIncrement() * cacheSize);
      if (limitValue != null && newValue > limitValue){
        newValue = limitValue;
      }
    }
    else{
      newValue = value - (getIncrement() * cacheSize);
      if (limitValue != null && newValue < limitValue){
        newValue = limitValue;
      }
    }
    setValue(newValue);
    save(db);

    this.cacheStart = value;
    if (orederType == SequenceOrderType.ORDER_POSITIVE){
      this.cacheEnd = newValue;
      if (limitValue == null || newValue != limitValue){
        --this.cacheEnd;
      }
    }
    else{
      this.cacheEnd = newValue;
      if (limitValue == null || newValue != limitValue){
        ++this.cacheEnd;
      }
    }
    firstCache = false;
  }
  
}