package com.orientechnologies.orient.core.command;

import java.util.Set;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 7/2/14
 */
public interface ODistributedCommand {
	Set<String> nodesToExclude();
}
