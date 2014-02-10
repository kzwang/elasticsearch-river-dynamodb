package org.elasticsearch.river.dynamodb;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.*;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.*;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 *
 */
public class DynamoDbRiver extends AbstractRiverComponent implements River {

    private final Client client;
    private final AmazonDynamoDBClient dynamoDBClient;


    private final String riverIndexName;
    private final String indexName;
    private final String typeName;
    private final int bulkSize;
    private volatile Thread indexerThread;
    private volatile boolean closed;
    private final TimeValue bulkFlushInterval;
    private volatile BulkProcessor bulkProcessor;
    private final int maxConcurrentBulk;
    private final String tableName;
    private final String idField;
    private final String updatedTimestampField;
    private final String deletedTimestampField;
    private final TimeValue interval;

    @Inject
    public DynamoDbRiver(RiverName riverName, RiverSettings settings, Client client, @RiverIndexName String riverIndexName) {
        super(riverName, settings);
        this.client = client;
        this.riverIndexName = riverIndexName;

        if (settings.settings().containsKey("dynamodb")) {
            Map<String, Object> dynamoDbSetting = (Map<String, Object>) settings.settings().get("dynamodb");

            String accessKey = XContentMapValues.nodeStringValue(dynamoDbSetting.get("access_key"), null);
            String secretKey = XContentMapValues.nodeStringValue(dynamoDbSetting.get("secret_key"), null);
            AWSCredentialsProvider credentials = new AWSCredentialsProviderChain(new StaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)));

            dynamoDBClient = new AmazonDynamoDBClient(credentials);

            String regionStr = XContentMapValues.nodeStringValue(dynamoDbSetting.get("region"), Regions.DEFAULT_REGION.getName());
            Regions region = null;
            for (Regions regions : Regions.values()) {
                if (regions.getName().equals(regionStr)) {
                    region = regions;
                    break;
                }
            }
            if (region == null) {
                logger.warn("Unable to find DynamoDB region");
                dynamoDBClient.setEndpoint(regionStr);  // use regionStr as endpoint directly, this should only happen in test or user has their own DynamoDB endpoint
            } else {
                dynamoDBClient.setRegion(Region.getRegion(region));
            }

            tableName = XContentMapValues.nodeStringValue(dynamoDbSetting.get("table_name"), null);
            idField = XContentMapValues.nodeStringValue(dynamoDbSetting.get("id_field"), "id");
            updatedTimestampField = XContentMapValues.nodeStringValue(dynamoDbSetting.get("updated_timestamp_field"), null);
            deletedTimestampField = XContentMapValues.nodeStringValue(dynamoDbSetting.get("deleted_timestamp_field"), null);
            indexName = XContentMapValues.nodeStringValue(dynamoDbSetting.get("index"), tableName);
            typeName = XContentMapValues.nodeStringValue(dynamoDbSetting.get("type"), tableName);
            bulkSize = XContentMapValues.nodeIntegerValue(dynamoDbSetting.get("bulk_size"), 100);
            bulkFlushInterval = TimeValue.parseTimeValue(XContentMapValues.nodeStringValue(
                    dynamoDbSetting.get("flush_interval"), "5s"), TimeValue.timeValueSeconds(5));
            maxConcurrentBulk = XContentMapValues.nodeIntegerValue(dynamoDbSetting.get("max_concurrent_bulk"), 1);
            interval = TimeValue.parseTimeValue(XContentMapValues.nodeStringValue(
                    dynamoDbSetting.get("interval"), "0s"), TimeValue.timeValueSeconds(0));


        } else {
            throw new ElasticsearchException("Unable to find DynamoDB river setting");
        }
    }

    @Override
    public void start() {
        logger.info("Start DynamoDB for table {}", tableName);
        if (!client.admin().indices().prepareExists(indexName).get().isExists()) {
            try {
                client.admin().indices().prepareCreate(indexName).get();
            } catch (Exception e) {
                logger.warn("failed to create index [{}], disabling river...", e, indexName);
                return;
            }
        }

        // Creating bulk processor
        this.bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                logger.debug("Going to execute new bulk composed of {} actions", request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                logger.debug("Executed bulk composed of {} actions", request.numberOfActions());
                if (response.hasFailures()) {
                    logger.warn("There was failures while executing bulk", response.buildFailureMessage());
                    if (logger.isDebugEnabled()) {
                        for (BulkItemResponse item : response.getItems()) {
                            if (item.isFailed()) {
                                logger.debug("Error for {}/{}/{} for {} operation: {}", item.getIndex(),
                                        item.getType(), item.getId(), item.getOpType(), item.getFailureMessage());
                            }
                        }
                    }
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                logger.warn("Error executing bulk", failure);
            }
        })
                .setBulkActions(bulkSize)
                .setConcurrentRequests(maxConcurrentBulk)
                .setFlushInterval(bulkFlushInterval)
                .build();


        indexerThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "dynamodb_river").newThread(new Indexer());
        indexerThread.start();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        logger.info("Close DynamoDB river");
        indexerThread.interrupt();

        if (bulkProcessor != null) {
            bulkProcessor.close();
        }

        closed = true;
    }


    private class Indexer implements Runnable {
        @Override
        public void run() {
            while (true) {
                if (closed) {
                    return;
                }


                Long now = new Date().getTime();
                Long lastChecked = getLastCheckTime();
                index(lastChecked, now);
                delete(lastChecked, now);
                setLastCheckTime(now);

                if (interval.getSeconds() > 0) {
                    try {
                        Thread.sleep(interval.getMillis());
                    } catch (InterruptedException e) {}
                } else {
                    return;
                }
            }
        }

        private void index(Long lastChecked, Long now) {
            Map<String, AttributeValue> lastEvaluatedKey = null;
            do {
                List<Map<String, AttributeValue>> result;
                ScanRequest scanRequest = new ScanRequest()
                        .withTableName(tableName)
                        .withLimit(bulkSize)
                        .withExclusiveStartKey(lastEvaluatedKey);


                Condition rangeKeyCondition = new Condition()
                        .withComparisonOperator(ComparisonOperator.BETWEEN)
                        .withAttributeValueList(new AttributeValue().withN(String.valueOf((lastChecked))), new AttributeValue().withN(now.toString()));

                scanRequest = scanRequest.addScanFilterEntry(updatedTimestampField, rangeKeyCondition);

                ScanResult scanResult = dynamoDBClient.scan(scanRequest);
                result = scanResult.getItems();
                lastEvaluatedKey = scanResult.getLastEvaluatedKey();

                for (Map<String, AttributeValue> item : result) {
                    processIndexItem(item);
                }
            } while (lastEvaluatedKey != null);

        }

        private void delete(Long lastChecked, Long now) {
            if (deletedTimestampField != null) {
                Map<String, AttributeValue> lastEvaluatedKey = null;
                do {
                    List<Map<String, AttributeValue>> result;
                    ScanRequest scanRequest = new ScanRequest()
                            .withTableName(tableName)
                            .withLimit(bulkSize)
                            .withExclusiveStartKey(lastEvaluatedKey);

                    Condition rangeKeyCondition = new Condition()
                            .withComparisonOperator(ComparisonOperator.BETWEEN)
                            .withAttributeValueList(new AttributeValue().withN(String.valueOf((lastChecked))), new AttributeValue().withN(now.toString()));

                    scanRequest = scanRequest.addScanFilterEntry(deletedTimestampField, rangeKeyCondition);

                    ScanResult scanResult = dynamoDBClient.scan(scanRequest);
                    result = scanResult.getItems();
                    lastEvaluatedKey = scanResult.getLastEvaluatedKey();

                    for (Map<String, AttributeValue> item : result) {
                        String id = null;
                        for (String name: item.keySet()) {
                            if (name.equals(idField)) {
                                AttributeValue attributeValue = item.get(name);
                                Object value = getAttributeValue(attributeValue);
                                if (value != null) {
                                    id = value.toString();
                                    break;
                                }
                                break;
                            }
                        }
                        if (id == null) {
                            throw new ElasticsearchException("Unable to find id for DynamoDB result");
                        }
                        bulkProcessor.add(new DeleteRequest(indexName, typeName, id));
                    }
                } while (lastEvaluatedKey != null);
            }
        }


        private void processIndexItem(Map<String, AttributeValue> item) {
            try {
                String id = null;
                XContentBuilder builder = jsonBuilder().startObject();
                for (String name: item.keySet()) {
                    AttributeValue attributeValue = item.get(name);
                    Object value = getAttributeValue(attributeValue);
                    if (value != null) {
                        builder.field(name, value);
                        if (name.equals(idField)) {
                            id = value.toString();
                        }
                    }
                }
                builder.endObject();
                if (id == null) {
                    throw new ElasticsearchException("Unable to find id for DynamoDB result");
                }
                bulkProcessor.add(new IndexRequest(indexName, typeName, id).source(builder));
            } catch (IOException e) {
                logger.error("Failed to convert DynamoDB result", e);
            }
        }

        private Object getAttributeValue(AttributeValue attributeValue) {
            Object value = null;
            if (attributeValue.getS() != null) {
                value = attributeValue.getS();
            } else if (attributeValue.getSS() != null && !attributeValue.getSS().isEmpty()) {
                value = attributeValue.getSS();
            } else if (attributeValue.getN() != null) {
                value = attributeValue.getN();
            } else if (attributeValue.getNS() != null && !attributeValue.getNS().isEmpty()) {
                value = attributeValue.getNS();
            }
            return value;
        }

        private void setLastCheckTime(final Long timeStamp) {
            Map<String, Object> statMap = Maps.newHashMap();
            statMap.put("lastChecked", timeStamp);
            client.prepareIndex(riverIndexName, riverName.getName(), "_stat").setSource(statMap).execute(new ActionListener<IndexResponse>() {
                @Override
                public void onResponse(IndexResponse indexResponse) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Last check time saved, {}", timeStamp);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    logger.error("Failed to record last checked time stamp", e);
                }
            });
        }

        private Long getLastCheckTime() {
            GetResponse getResponse = client.prepareGet(riverIndexName, riverName.getName(), "_stat").get();
            if (getResponse.isExists()) {
                Map<String, Object> statMap = getResponse.getSourceAsMap();
                if (statMap.containsKey("lastChecked")) {
                    return (Long) statMap.get("lastChecked");
                }
            }
            return 0l;
        }
    }

}
