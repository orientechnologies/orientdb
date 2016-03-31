/*
 *
 *  *  Copyright 2016 Orient Technologies LTD (info(at)orientdb.com)
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
package com.orientechnologies.security.password;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OInvalidPasswordException;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerConfigurationManager;
import com.orientechnologies.orient.server.security.OPasswordValidator;


/**
 * Provides a default implementation for validating passwords.
 * 
 * @author S. Colin Leister
 * 
 */
public class ODefaultPasswordValidator implements OPasswordValidator
{
	private boolean _Enabled = true;
	private boolean _IgnoreUUID = true;
	private int _MinLength = 0;
	private Pattern _HasNumber;
	private Pattern _HasSpecial;
	private Pattern _HasUppercase;
	private Pattern _IsUUID = Pattern.compile("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");
	
	// OSecurityComponent
	public void active() { }

	// OSecurityComponent
	public void config(final OServer oServer, final OServerConfigurationManager serverCfg, final ODocument jsonConfig)
	{
		try
		{
			if(jsonConfig.containsField("enabled"))
			{
				_Enabled = jsonConfig.field("enabled");
			}

			if(jsonConfig.containsField("ignoreUUID"))
			{
				_IgnoreUUID = jsonConfig.field("ignoreUUID");
			}

			if(jsonConfig.containsField("minimumLength"))
			{
				_MinLength = jsonConfig.field("minimumLength");
			}

			if(jsonConfig.containsField("numberRegEx"))
			{
				_HasNumber = Pattern.compile((String)jsonConfig.field("numberRegEx"));
			}

			if(jsonConfig.containsField("specialRegEx"))
			{
				_HasSpecial = Pattern.compile((String)jsonConfig.field("specialRegEx"));
			}

			if(jsonConfig.containsField("uppercaseRegEx"))
			{
				_HasUppercase = Pattern.compile((String)jsonConfig.field("uppercaseRegEx"));
			}
		}
		catch(Exception ex)
		{
			OLogManager.instance().error(this, "ODefaultPasswordValidator.config() Exception: %s", ex.getMessage());
		}
	}

	// OSecurityComponent
	public void dispose() { }

	// OSecurityComponent
	public boolean isEnabled() { return _Enabled; }

	
	// OPasswordValidator
	public void validatePassword(final String password) throws OInvalidPasswordException
	{
		if(!_Enabled) return;
		
		if(password != null && !password.isEmpty())
		{
			if(_IgnoreUUID && isUUID(password)) return;
			
			if(password.length() < _MinLength)
			{
				OLogManager.instance().debug(this, "ODefaultPasswordValidator.validatePassword() Password length (%d) is too short", password.length());
				throw new OInvalidPasswordException("Password length is too short.  Minimum password length is " + _MinLength);
			}

			if(_HasNumber != null && !isValid(_HasNumber, password))
			{
				OLogManager.instance().debug(this, "ODefaultPasswordValidator.validatePassword() Password requires a minimum count of numbers");
				throw new OInvalidPasswordException("Password requires a minimum count of numbers");
			}

			if(_HasSpecial != null && !isValid(_HasSpecial, password))
			{
				OLogManager.instance().debug(this, "ODefaultPasswordValidator.validatePassword() Password requires a minimum count of special characters");
				throw new OInvalidPasswordException("Password requires a minimum count of special characters");
			}

			if(_HasUppercase != null && !isValid(_HasUppercase, password))
			{
				OLogManager.instance().debug(this, "ODefaultPasswordValidator.validatePassword() Password requires a minimum count of uppercase characters");
				throw new OInvalidPasswordException("Password requires a minimum count of uppercase characters");
			}
		}
		else
		{
			OLogManager.instance().debug(this, "ODefaultPasswordValidator.validatePassword() Password is null or empty");			
			throw new OInvalidPasswordException("ODefaultPasswordValidator.validatePassword() Password is null or empty");
		}
	}

	private boolean isValid(final Pattern pattern, final String password)
	{
		return pattern.matcher(password).find();
	}

	private boolean isUUID(final String password)
	{
		return _IsUUID.matcher(password).find();
	}
}
