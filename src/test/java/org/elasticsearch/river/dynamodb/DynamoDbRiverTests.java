package org.elasticsearch.river.dynamodb;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.*;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.*;

@ElasticsearchIntegrationTest.ClusterScope(transportClientRatio = 0.0, numNodes = 1, scope = ElasticsearchIntegrationTest.Scope.TEST)
public class DynamoDbRiverTests extends ElasticsearchIntegrationTest {

    @Test
    public void test_dynamodb_river() throws IOException, InterruptedException {

        String tableName = randomAsciiOfLengthBetween(10, 50).toLowerCase();
        AmazonDynamoDBClient dynamoDBClient = getDynamoClient(tableName);

        // add test items to dynamodb
        Set<String> idSet = Sets.newHashSet();
        idSet.addAll(addTestItems(dynamoDBClient, tableName, randomIntBetween(1, 50)));

        Map<String, String> dynamoDbSetting = Maps.newHashMap();
        dynamoDbSetting.put("access_key", "test");
        dynamoDbSetting.put("secret_key", "test");
        dynamoDbSetting.put("region", "http://localhost:8000");  // test use DynamoDB Local
        dynamoDbSetting.put("table_name", tableName);
        dynamoDbSetting.put("updated_timestamp_field", "updated");
        dynamoDbSetting.put("deleted_timestamp_field", "deleted");
        dynamoDbSetting.put("interval", "1s");
        dynamoDbSetting.put("bulk_size", "2");  // less bulk size so items can be indexed when doing test
        dynamoDbSetting.put("flush_interval", "1s");  // less bulk interval so items can be indexed when doing test

        XContentBuilder riverBuilder = jsonBuilder().startObject();
        riverBuilder.field("type", "dynamodb");
        riverBuilder.field("dynamodb", dynamoDbSetting);
        riverBuilder.endObject();


        client().prepareIndex("_river", "dynamodb", "_meta").setSource(riverBuilder).get();

        Thread.sleep(10000);

        client().admin().indices().prepareRefresh(tableName).get();
        long count1 = client().prepareCount(tableName).get().getCount();
        assertThat(count1, equalTo((long) idSet.size()));

        // load more test data to dynamodb
        idSet.addAll(addTestItems(dynamoDBClient, tableName, randomIntBetween(1, 50)));

        Thread.sleep(10000);

        client().admin().indices().prepareRefresh(tableName).get();
        long count2 = client().prepareCount(tableName).get().getCount();
        assertThat(count2, equalTo((long) idSet.size()));


        // test delete
        int toDelete = randomIntBetween(1, idSet.size() - 1);
        for (int i = 0; i < toDelete; i ++) {
            String id = idSet.iterator().next();
            deleteFromDynamoDB(dynamoDBClient, tableName, id);
            idSet.remove(id);
        }

        Thread.sleep(10000);

        client().admin().indices().prepareRefresh(tableName).get();
        long count3 = client().prepareCount(tableName).get().getCount();
        assertThat(count3, equalTo((long) idSet.size()));



    }

    private Set<String> addTestItems(AmazonDynamoDBClient dynamoDBClient, String tableName, int size) {
        Set<String> idSet = Sets.newHashSet();
        for (int p = 0; p < size; p ++) {
            Map<String, AttributeValue> item = Maps.newHashMap();
            int fields = randomIntBetween(10, 20);
            for (int i = 0; i < fields; i ++) {
                int t = randomIntBetween(1, 4);
                switch (t){
                    case 1:
                        item.put(randomAsciiOfLengthBetween(1, 10), new AttributeValue().withS(randomAsciiOfLengthBetween(1, 20)));
                        break;
                    case 2:
                        item.put(randomAsciiOfLengthBetween(1, 10), new AttributeValue().withN(String.valueOf(randomInt())));
                        break;
                    case 3:
                        Set<String> s = Sets.newHashSet();
                        int ss = randomIntBetween(5, 20);
                        for (int a = 0; a < ss; a ++) {
                            s.add(randomAsciiOfLengthBetween(1, 20));
                        }
                        item.put(randomAsciiOfLengthBetween(1, 10), new AttributeValue().withSS(s));
                        break;
                    case 4:
                        Set<String> n = Sets.newHashSet();
                        int ns = randomIntBetween(5, 20);
                        for (int a = 0; a < ns; a ++) {
                            n.add(String.valueOf(randomIntBetween(1, 1000)));
                        }
                        item.put(randomAsciiOfLengthBetween(1, 10), new AttributeValue().withNS(n));
                        break;
                }
            }
            String id = randomAsciiOfLengthBetween(1, 50);
            idSet.add(id);
            item.put("id", new AttributeValue().withS(id));
            item.put("updated", new AttributeValue().withN(String.valueOf(new Date().getTime())));
            PutItemRequest putItemRequest = new PutItemRequest()
                    .withTableName(tableName)
                    .withItem(item);
            dynamoDBClient.putItem(putItemRequest);
        }
        return idSet;
    }

    private void deleteFromDynamoDB(AmazonDynamoDBClient dynamoDBClient, String tableName, String id) {
        UpdateItemRequest updateItemRequest = new UpdateItemRequest()
                .withTableName(tableName)
                .addKeyEntry("id", new AttributeValue().withS(id))
                .addAttributeUpdatesEntry("deleted", new AttributeValueUpdate().withValue(new AttributeValue().withN(String.valueOf(new Date().getTime()))));

        dynamoDBClient.updateItem(updateItemRequest);


    }

    private AmazonDynamoDBClient getDynamoClient(String tableName) {
        AWSCredentialsProvider credentials = new AWSCredentialsProviderChain(new StaticCredentialsProvider(new BasicAWSCredentials("test", "test")));
        AmazonDynamoDBClient amazonDynamoDBClient = new AmazonDynamoDBClient(credentials);
        amazonDynamoDBClient.setEndpoint("http://localhost:8000");

        ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput()
                .withReadCapacityUnits(5L)
                .withWriteCapacityUnits(10L);
        CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName).withProvisionedThroughput(provisionedThroughput);
        ArrayList<AttributeDefinition> attributeDefinitions= new ArrayList<AttributeDefinition>();
        attributeDefinitions.add(new AttributeDefinition().withAttributeName("id").withAttributeType("S"));
        createTableRequest.setAttributeDefinitions(attributeDefinitions);

        ArrayList<KeySchemaElement> tableKeySchema = new ArrayList<KeySchemaElement>();
        tableKeySchema.add(new KeySchemaElement().withAttributeName("id").withKeyType(KeyType.HASH));
        createTableRequest.setKeySchema(tableKeySchema);
        amazonDynamoDBClient.createTable(createTableRequest);
        return amazonDynamoDBClient;

    }
}
