package org.elasticsearch.river.dynamodb;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

/**
 *
 */
public class DynamoDbRiverModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(River.class).to(DynamoDbRiver.class).asEagerSingleton();
    }
}
