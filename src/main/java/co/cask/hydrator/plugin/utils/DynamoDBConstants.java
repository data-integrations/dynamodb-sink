/*
 * Copyright © 2017 Cask Data, Inc.
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
package co.cask.hydrator.plugin.utils;

/**
 * Contains constants used for the Hadoop to DynamoDB connection.
 */
public interface DynamoDBConstants {
  // Credentials
  String DYNAMODB_ACCESS_KEY = "dynamodb.awsAccessKey";
  String DYNAMODB_SECRET_KEY = "dynamodb.awsSecretAccessKey";
  String PRIMARY_KEY_FIELDS = "dynamodb.primaryKey.attribute.fields";
  String PRIMARY_KEY_TYPES = "dynamodb.primaryKey.types";

  String ENDPOINT = "dynamodb.endpoint";
  String REGION_ID = "dynamodb.regionid";

  String OUTPUT_TABLE_NAME = "dynamodb.output.tableName";
  String READ_CAPACITY_UNITS = "dynamodb.read.capacity.units";
  String WRITE_CAPACITY_UNITS = "dynamodb.write.capacity.units";
  String DEFAULT_WRITE_CAPACITY_UNITS = "100";
  String DEFAULT_READ_CAPACITY_UNITS = "100";
  /* BatchWriteItem allows: (http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html)
   max batch = 25
   max individual item size = 400 KB
   total request size = 16MB */
  int MAX_ALLOWABLE_BYTE_SIZE = 400 * 1024;
  int MAX_WRITE_BATCH_SIZE = 25;
  int MAX_BATCH_SIZE_BYTES = 16 * 1024 * 1024;
  int MAX_RETIRES = 3;
}
