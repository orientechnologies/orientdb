/*
 *
 *  *  Copyright 2016 OrientDB LTD
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
package com.orientechnologies.agent.security.authenticator;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.symmetrickey.OSymmetricKeyConfig;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;
import com.orientechnologies.orient.server.security.OSecurityAuthenticatorException;

/**
 * Implements the OSymmetricKeyConfig interface for OServerUserConfiguration users.
 * The constructor takes the user's JSON document and looks for a "properties" field.
 * The "properties" field should be a JSON document containing the OSymmetricKey-specific fields.
 * 
 * @author S. Colin Leister
 * 
 */
public class OSecuritySymmetricKeyUser extends OServerUserConfiguration implements OSymmetricKeyConfig {
  private String keyString;
  private String keyFile;
  private String keyAlgorithm;
  private String keystoreFile;
  private String keystorePassword;
  private String keystoreKeyAlias;
  private String keystoreKeyPassword;

  // OSymmetricKeyConfig
  public String getKeyString() { return keyString; }
  public String getKeyFile() { return keyFile; }
  public String getKeyAlgorithm() { return keyAlgorithm; }
  public String getKeystoreFile() { return keystoreFile; }
  public String getKeystorePassword() { return keystorePassword; }
  public String getKeystoreKeyAlias() { return keystoreKeyAlias; }
  public String getKeystoreKeyPassword() { return keystoreKeyPassword; }
  // OSymmetricKeyConfig
  public boolean usesKeyString() { return keyString != null && !keyString.isEmpty() && keyAlgorithm != null && !keyAlgorithm.isEmpty(); }
  public boolean usesKeyFile() { return keyFile != null && !keyFile.isEmpty() && keyAlgorithm != null && !keyAlgorithm.isEmpty(); }
  public boolean usesKeystore() { return keystoreFile != null && !keystoreFile.isEmpty() && keystoreKeyAlias != null && !keystoreKeyAlias.isEmpty(); }
  //////////

  public OSecuritySymmetricKeyUser() {
  }

  public OSecuritySymmetricKeyUser(final ODocument userDoc) {
    if(userDoc == null) throw new OSecurityAuthenticatorException("OSecuritySymmetricKeyUser() userDoc is null");

    final String username = userDoc.field("username");
    final String resources = userDoc.field("resources");

    if(username == null) throw new OSecurityAuthenticatorException("OSecuritySymmetricKeyUser() username is null");
    if(resources == null) throw new OSecurityAuthenticatorException("OSecuritySymmetricKeyUser() resources is null");

    super.name = username;
    super.resources = resources;

    String password = userDoc.field("password");
    if (password == null) super.password = "";

    ODocument props = userDoc.field("properties");

    if(props == null) throw new OSecurityAuthenticatorException("OSecuritySymmetricKeyUser() properties is null");

    this.keyString = props.field("key");
    
    // "keyString" has priority over "keyFile" and "keystore".
    if(this.keyString != null) {
    	// If "key" is used, "keyAlgorithm" is also required.
    	this.keyAlgorithm = props.field("keyAlgorithm");

    	if(this.keyAlgorithm == null) throw new OSecurityAuthenticatorException("OSecuritySymmetricKeyUser() keyAlgorithm is required with key");
    }
    else {
      this.keyFile = props.field("keyFile");
      
      // "keyFile" has priority over "keyStore".      
      
      if(this.keyFile != null) {
        // If "keyFile" is used, "keyAlgorithm" is also required.
        this.keyAlgorithm = props.field("keyAlgorithm");

        if(this.keyAlgorithm == null) throw new OSecurityAuthenticatorException("OSecuritySymmetricKeyUser() keyAlgorithm is required with keyFile");
      }
      else {
        ODocument ksDoc = props.field("keyStore");
        
        if(ksDoc == null) throw new OSecurityAuthenticatorException("OSecuritySymmetricKeyUser() key, keyFile, and keyStore cannot all be null");

        this.keystoreFile = ksDoc.field("file");
        this.keystorePassword = ksDoc.field("passsword");
        this.keystoreKeyAlias = ksDoc.field("keyAlias");
        this.keystoreKeyPassword = ksDoc.field("keyPassword");
        
        if(this.keystoreFile == null) throw new OSecurityAuthenticatorException("OSecuritySymmetricKeyUser() keyStore.file is required");
        if(this.keystoreKeyAlias == null) throw new OSecurityAuthenticatorException("OSecuritySymmetricKeyUser() keyStore.keyAlias is required");
      }
    }
  }
}
