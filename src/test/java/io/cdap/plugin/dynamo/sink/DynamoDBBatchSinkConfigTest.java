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
package io.cdap.plugin.dynamo.sink;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Unit Tests for DynamoDBConfig.
 */
public class DynamoDBBatchSinkConfigTest {
  private static final String MOCK_STAGE = "mockStage";
  private static final Schema SCHEMA = Schema.recordOf(
    "schema",
    Schema.Field.of("Id", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("Region", Schema.of(Schema.Type.STRING))
  );
  private static final DynamoDBBatchSinkConfig VALID_CONFIG = new DynamoDBBatchSinkConfig(
    "Referencename",
    "testKey",
    "testKey",
    "us-east-1",
    null,
    "dynamoDBTest",
    "Id:N,Region:RANGE",
    "Id:HASH,Region:RANGE",
    null,
    null
  );

  @Test
  public void testValidConfig() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validateTableName(failureCollector);
    VALID_CONFIG.validatePrimaryKey(failureCollector, SCHEMA);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testInvalidTableNameLength() {
    DynamoDBBatchSinkConfig config = DynamoDBBatchSinkConfig.builder(VALID_CONFIG)
      .setTableName("tt")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validateTableName(failureCollector);
    assertValidationFail(failureCollector, CauseAttributes.STAGE_CONFIG, DynamoDBBatchSinkConfig.TABLE_NAME);
  }

  @Test
  public void testInvalidTableName() {
    DynamoDBBatchSinkConfig config = DynamoDBBatchSinkConfig.builder(VALID_CONFIG)
      .setTableName("table%^name")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validateTableName(failureCollector);
    assertValidationFail(failureCollector, CauseAttributes.STAGE_CONFIG, DynamoDBBatchSinkConfig.TABLE_NAME);
  }

  @Test
  public void testMoreThankTwoPrimaryKeys() {
    DynamoDBBatchSinkConfig config = DynamoDBBatchSinkConfig.builder(VALID_CONFIG)
      .setPrimaryKeyFields("Id:N,Region:RANGE,State:RANGE")
      .setPrimaryKeyTypes("Id:HASH,Region:RANGE,State:RANGE")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validatePrimaryKey(failureCollector, SCHEMA);
    assertValidationFail(failureCollector, CauseAttributes.STAGE_CONFIG, DynamoDBBatchSinkConfig.PRIMARY_KEY_FIELDS);
  }

  @Test
  public void testInvalidPrimaryKeyType() {
    DynamoDBBatchSinkConfig config = DynamoDBBatchSinkConfig.builder(VALID_CONFIG)
      .setPrimaryKeyFields("Id:N,Region:RANGE")
      .setPrimaryKeyTypes("Region:RANGE,State:RANGE")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validatePrimaryKey(failureCollector, SCHEMA);
    assertValidationFail(failureCollector, CauseAttributes.STAGE_CONFIG, DynamoDBBatchSinkConfig.PRIMARY_KEY_TYPES);
  }

  @Test
  public void testInvalidPrimaryKeys() {
    Schema schema = Schema.recordOf(
      "schema",
      Schema.Field.of("Id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("Region", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
    );

    DynamoDBBatchSinkConfig config = DynamoDBBatchSinkConfig.builder(VALID_CONFIG)
      .setPrimaryKeyFields("Id:N,Region:RANGE")
      .setPrimaryKeyTypes("Id:HASH,Region:RANGE")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validatePrimaryKey(failureCollector, schema);
    assertValidationFail(failureCollector, CauseAttributes.INPUT_SCHEMA_FIELD, "Region");
  }

  private void assertValidationFail(MockFailureCollector failureCollector, String attributeName,
                                    String attributeValue) {
    List<ValidationFailure> failureList = failureCollector.getValidationFailures();
    Assert.assertEquals(1, failureList.size());

    ValidationFailure failure = failureList.get(0);
    List<ValidationFailure.Cause> causeList = failure.getCauses();
    Assert.assertEquals(1, causeList.size());

    ValidationFailure.Cause cause = causeList.get(0);
    Assert.assertEquals(attributeValue, cause.getAttribute(attributeName));
  }
}
