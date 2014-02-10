package org.elasticsearch.plugin.river.dynamodb;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;
import org.elasticsearch.river.dynamodb.DynamoDbRiverModule;

/**
 *
 */
public class DynamoDbPlugin extends AbstractPlugin {

    @Inject
    public DynamoDbPlugin() {
    }

    @Override
    public String name() {
        return "river-dynamodb";
    }

    @Override
    public String description() {
        return "DynamoDB River Plugin";
    }

    public void onModule(RiversModule module) {
        module.registerRiver("dynamodb", DynamoDbRiverModule.class);
    }
}
