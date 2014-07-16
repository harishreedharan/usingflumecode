/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package usingflume.ch05;

import com.google.common.base.Preconditions;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

public class S3Sink extends AbstractSink implements Configurable {

  private static String CONF_AWS_KEYID = "awsAccessKeyId";
  private static String CONF_SECRET_KEY = "awsSecretKey";
  private static String CONF_BUCKET = "bucket";
  private static String CONF_BATCHSIZE = "batchSize";

  private static int DEFAULT_BATCHSIZE = 1000;

  private String awsAccessKeyId;
  private String awsSecretKey;
  private String bucket;
  private int batchSize;

  @Override
  public Status process() throws EventDeliveryException {
    return null;
  }

  @Override
  public void configure(Context context) {
    awsAccessKeyId = context.getString(CONF_AWS_KEYID);
    Preconditions.checkArgument(
      awsAccessKeyId != null && !awsAccessKeyId.isEmpty(),
      "AWS Key Id is required");

    awsSecretKey = context.getString(CONF_SECRET_KEY);
    Preconditions.checkArgument(
      awsSecretKey != null && !awsSecretKey.isEmpty(),
      "AWS Secret Key must be specified");

    bucket = context.getString(CONF_BUCKET);
    Preconditions.checkArgument(bucket != null && !bucket.isEmpty(),
      "Bucket name must be specified");

    batchSize = context.getInteger(CONF_BATCHSIZE, DEFAULT_BATCHSIZE);
  }
}
