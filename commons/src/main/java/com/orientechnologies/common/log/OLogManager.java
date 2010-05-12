package com.orientechnologies.common.log;

import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;

import com.orientechnologies.common.exception.OException;

public class OLogManager {

	private static final String				SYSPROPERTY_LOG_LEVEL	= "orient.log.level";

	private Level											level;
	private boolean										warn									= false;
	private boolean										info									= false;
	private boolean										debug									= false;

	private static final OLogManager	instance							= new OLogManager();
	private static final DateFormat		dateFormat						= new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS");

	public OLogManager() {
		String configLevel = System.getProperty(SYSPROPERTY_LOG_LEVEL);

		level = configLevel != null ? Level.parse(configLevel.toUpperCase()) : Level.INFO;
		if (level.equals(Level.FINER) || level.equals(Level.FINE) || level.equals(Level.FINEST))
			debug = info = warn = true;
		else if (level.equals(Level.INFO))
			info = warn = true;
		else if (level.equals(Level.WARNING))
			warn = true;
	}

	public PrintStream log(final Object iRequester, final Level iLevel, final String iMessage, final Throwable iException,
			final Object... iAdditionalArgs) {
		final PrintStream stream = iLevel.equals(Level.SEVERE) ? System.err : System.out;

		final StringBuilder buffer = new StringBuilder();
		buffer.append(dateFormat.format(new Date()));
		buffer.append(' ');
		buffer.append(iLevel.getName().substring(0, 4));
		if (iRequester != null) {
			buffer.append(" [");
			buffer.append(iRequester.getClass().getSimpleName());
			buffer.append("]");
		}
		buffer.append(' ');

		// FORMAT THE MESSAGE
		try {
			buffer.append(String.format(iMessage, iAdditionalArgs));
		} catch (Exception e) {
			buffer.append(iMessage);
		}

		stream.println(buffer);

		if (iException != null)
			iException.printStackTrace(stream);

		return stream;
	}

	public void debug(final Object iRequester, final String iMessage, final Object... iAdditionalArgs) {
		if (isDebugEnabled())
			log(iRequester, Level.FINE, iMessage, null, iAdditionalArgs);
	}

	public void debug(final Object iRequester, final String iMessage, final Throwable iException, final Object... iAdditionalArgs) {
		if (isDebugEnabled())
			log(iRequester, Level.FINE, iMessage, iException, iAdditionalArgs);
	}

	public void info(final Object iRequester, final String iMessage, final Object... iAdditionalArgs) {
		if (isInfoEnabled())
			log(iRequester, Level.INFO, iMessage, null, iAdditionalArgs);
	}

	public void info(final Object iRequester, final String iMessage, final Throwable iException, final Object... iAdditionalArgs) {
		if (isInfoEnabled())
			log(iRequester, Level.INFO, iMessage, iException, iAdditionalArgs);
	}

	public void warn(final Object iRequester, final String iMessage, final Object... iAdditionalArgs) {
		if (isWarnEnabled())
			log(iRequester, Level.WARNING, iMessage, null, iAdditionalArgs);
	}

	public void warn(final Object iRequester, final String iMessage, final Throwable iException, final Object... iAdditionalArgs) {
		if (isWarnEnabled())
			log(iRequester, Level.WARNING, iMessage, iException, iAdditionalArgs);
	}

	public void error(final Object iRequester, final String iMessage, final Object... iAdditionalArgs) {
		log(iRequester, Level.SEVERE, iMessage, null, iAdditionalArgs);
	}

	public void error(final Object iRequester, final String iMessage, final Throwable iException, final Object... iAdditionalArgs) {
		log(iRequester, Level.SEVERE, iMessage, iException, iAdditionalArgs);
	}

	public void config(final Object iRequester, final String iMessage, final Object... iAdditionalArgs) {
		log(iRequester, Level.CONFIG, iMessage, null, iAdditionalArgs).flush();
	}

	public void error(final Object iRequester, final String iMessage, final Throwable iException,
			final Class<? extends OException> iExceptionClass, final Object... iAdditionalArgs) {
		error(iRequester, iMessage, iException, iAdditionalArgs);

		try {
			throw iExceptionClass.getConstructor(String.class, Throwable.class).newInstance(iMessage, iException);
		} catch (NoSuchMethodException e) {
		} catch (IllegalArgumentException e) {
		} catch (SecurityException e) {
		} catch (InstantiationException e) {
		} catch (IllegalAccessException e) {
		} catch (InvocationTargetException e) {
		}
	}

	public void error(final Object iRequester, final String iMessage, final Class<? extends OException> iExceptionClass) {
		error(iRequester, iMessage, (Throwable) null);

		try {
			throw iExceptionClass.getConstructor(String.class).newInstance(iMessage);
		} catch (IllegalArgumentException e) {
		} catch (SecurityException e) {
		} catch (InstantiationException e) {
		} catch (IllegalAccessException e) {
		} catch (InvocationTargetException e) {
		} catch (NoSuchMethodException e) {
		}
	}

	public boolean isDebugEnabled() {
		return debug;
	}

	public boolean isInfoEnabled() {
		return info;
	}

	public boolean isWarnEnabled() {
		return warn;
	}

	public static OLogManager instance() {
		return instance;
	}

	@SuppressWarnings("unchecked")
	public void exception(final String iMessage, final Exception iNestedException, final Class<? extends OException> iExceptionClass,
			final Object... iAdditionalArgs) throws OException {
		// FORMAT THE MESSAGE
		String msg = String.format(iMessage, iAdditionalArgs);

		Constructor<OException> c;
		OException exceptionToThrow = null;
		try {
			if (iNestedException != null) {
				c = (Constructor<OException>) iExceptionClass.getConstructor(String.class, Throwable.class);
				exceptionToThrow = c.newInstance(msg, iNestedException);
			}
		} catch (Exception e) {
		}

		if (exceptionToThrow == null)
			try {
				c = (Constructor<OException>) iExceptionClass.getConstructor(String.class);
				exceptionToThrow = c.newInstance(msg);
			} catch (SecurityException e1) {
			} catch (NoSuchMethodException e1) {
			} catch (IllegalArgumentException e1) {
			} catch (InstantiationException e1) {
			} catch (IllegalAccessException e1) {
			} catch (InvocationTargetException e1) {
			}

		if (exceptionToThrow != null)
			throw exceptionToThrow;
		else
			throw new IllegalArgumentException("Can't create the exception of type: " + iExceptionClass);
	}
}
