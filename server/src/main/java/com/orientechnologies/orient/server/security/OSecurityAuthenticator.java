/*
 *
 *  *  Copyright 2016 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.server.security;

import javax.security.auth.Subject;

//import com.orientechnologies.orient.core.record.impl.ODocument;
//import com.orientechnologies.orient.server.OServer;
//import com.orientechnologies.orient.server.config.OServerConfigurationManager;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;

/**
 * Provides an interface for creating security authenticators.
 * 
 * @author S. Colin Leister
 * 
 */
public interface OSecurityAuthenticator extends OSecurityComponent
{
	// Returns the actual username if successful, null otherwise.
	// Some token-based authentication (e.g., SPNEGO tokens have the user's name embedded in the service ticket).
	String authenticate(final String username, final String password);
	
	String getAuthenticationHeader(final String databaseName);
	
	Subject getClientSubject();
	
	// Returns the name of this OSecurityAuthenticator.
	String getName();
	
	OServerUserConfiguration getUser(final String username);

	boolean isAuthorized(final String username, final String resource);

	boolean isSingleSignOnSupported();
}
