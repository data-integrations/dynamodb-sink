/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin;

import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit Tests for DynamoDBConfig.
 */
public class DynamoDBBatchSinkConfigTest {

  @Test
  public void testInvalidTableNameLength() throws Exception {
    BatchDynamoDBSink.DynamoDBConfig config = new
      BatchDynamoDBSink.DynamoDBConfig("Referencename", "testKey", "testkey", "us-east-1", null, "tt", "Id:N",
                                       "Id:HASH", null, null);
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
    try {
      new BatchDynamoDBSink(config).configurePipeline(configurer);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Table name 'tt' does not follow the DynamoDB naming rules. Table name must be between 3 " +
                            "and 255 characters long.", e.getMessage());
    }
  }

  @Test
  public void testInvalidTableName() throws Exception {
    BatchDynamoDBSink.DynamoDBConfig config = new
      BatchDynamoDBSink.DynamoDBConfig("Referencename", "testKey", "testkey", "us-east-1", null, "table%^name", "Id:N",
                                       "Id:HASH", null, null);
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
    try {
      new BatchDynamoDBSink(config).configurePipeline(configurer);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Table name 'table%^name' does not follow the DynamoDB naming rules. Table names can " +
                            "contain only the following characters: 'a-z, A-Z, 0-9, underscore(_), dot(.) and dash(-)" +
                            "'.", e.getMessage());
    }
  }

  @Test
  public void testMoreThankTwoPrimaryKeys() {
    BatchDynamoDBSink.DynamoDBConfig config = new
      BatchDynamoDBSink.DynamoDBConfig("Referencename", "table%^name", "testKey", "us-east-1", null, "dynamoDBTest",
                                       "Id:N,Region:RANGE,State:RANGE", "Id:HASH,Region:RANGE,State:RANGE", null, null);
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
    try {
      new BatchDynamoDBSink(config).configurePipeline(configurer);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Primary key can have only 2 values HASH and RANGE.", e.getMessage());
    }
  }

  @Test
  public void testInvalidPrimaryKeys() {
    BatchDynamoDBSink.DynamoDBConfig config = new
      BatchDynamoDBSink.DynamoDBConfig("Referencename", "table%^name", "testKey", "us-east-1", null, "dynamoDBTest",
                                       "Id:N,Region:RANGE", "Region:RANGE,State:RANGE", null, null);
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
    try {
      new BatchDynamoDBSink(config).configurePipeline(configurer);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Please specify attribute value and key types for the same primary keys. Key type for [Id] " +
                            "is not specified.", e.getMessage());
    }
  }
}
