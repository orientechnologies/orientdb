package com.orientechnologies.website.configuration;

import com.orientechnologies.website.events.EventInternal;
import com.orientechnologies.website.services.reactor.GitHubHandler;
import com.orientechnologies.website.services.reactor.GitHubIssueImporter;
import com.orientechnologies.website.services.reactor.ReactorMSG;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.core.spec.Reactors;
import reactor.event.Event;

import javax.annotation.PostConstruct;
import java.util.List;

import static reactor.event.selector.Selectors.$;

/**
 * Created by Enrico Risa on 24/10/14.
 */
@Configuration
public class ReactorConfig {

    @Autowired
    private GitHubIssueImporter hubIssueImporter;

    @Autowired
    protected List<GitHubHandler<Event<?>>> handlers;

    @Autowired
    protected List<EventInternal<?>> internalEvents;

    @Autowired
    private Reactor reactor;

    @PostConstruct
    public void startup() {

        reactor.on($(ReactorMSG.ISSUE_IMPORT), hubIssueImporter);
        for (GitHubHandler<Event<?>> handler : handlers) {
            for (String s : handler.handleWhat()) {
                reactor.on($(s), handler);
            }
        }
        for (EventInternal<?> event : internalEvents) {
            reactor.on($(event.handleWhat()), event);
        }
    }

    @Bean
    Environment env() {
        return new Environment();
    }

    @Bean
    Reactor createReactor(Environment env) {
        return Reactors.reactor().env(env).dispatcher(Environment.THREAD_POOL).get();
    }

}
