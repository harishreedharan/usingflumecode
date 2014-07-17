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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicLong;

public class S3Sink extends AbstractSink implements Configurable {

  private String objPrefix;
  private final AtomicLong suffix = new AtomicLong(System
    .currentTimeMillis());
  private String awsAccessKeyId;
  private String awsSecretKey;
  private String bucket;
  private int batchSize;
  private String endPoint;

  private AmazonS3 connection;
  // 64K buffer.
  private final byte[] buffer = new byte[64 * 1024];

  @Override
  public void start() {
    // Set up Amazon S3 client.
    AWSCredentials credentials = new BasicAWSCredentials(
      awsAccessKeyId, awsSecretKey);
    ClientConfiguration config = new ClientConfiguration();
    config.setProtocol(Protocol.HTTP);
    connection = new AmazonS3Client(credentials, config);
    connection.setEndpoint(endPoint);
    if (!connection.doesBucketExist(bucket)) {
      connection.createBucket(bucket);
    }
    super.start();
  }

  @Override
  public synchronized void stop() {
    super.stop();
  }

  @Override
  public Status process() throws EventDeliveryException {
    Status status = Status.BACKOFF;
    Transaction tx = null;
    final ByteArrayOutputStream data
      = new ByteArrayOutputStream(64 * 1024);
    try {
      tx = getChannel().getTransaction();
      tx.begin();
      int i = 0;
      for (; i < batchSize; i++) {
        Event e = getChannel().take();
        if (e == null) {
          break;
        }
        data.write(e.getBody());
      }
      if (i != 0) {
        connection.putObject(bucket,
          objPrefix + suffix.incrementAndGet(),
          new ByteArrayInputStream(data.toByteArray()),
          new ObjectMetadata());
        status = Status.READY;
      }
      tx.commit();
    } catch (Exception e) {
      if (tx != null) {
        tx.rollback();
      }
      throw new EventDeliveryException("Error while processing " +
        "data", e);
    } finally {
      if (tx != null) {
        tx.close();
      }
    }
    return status;
  }

  @Override
  public void configure(Context context) {
    awsAccessKeyId = context.getString("awsAccessKeyId");
    Preconditions.checkArgument(
      awsAccessKeyId != null && !awsAccessKeyId.isEmpty(),
      "AWS Key Id is required");

    awsSecretKey = context.getString("awsSecretKey");
    Preconditions.checkArgument(
      awsSecretKey != null && !awsSecretKey.isEmpty(),
      "AWS Secret Key must be specified");

    bucket = context.getString("bucket");
    Preconditions.checkArgument(bucket != null && !bucket.isEmpty(),
      "Bucket name must be specified");

    endPoint = context.getString("batchSize");
    Preconditions.checkArgument(endPoint != null && !endPoint.isEmpty(),
      "Endpoint cannot be null");

    batchSize = context.getInteger("endPoint", 1000);
    objPrefix = context.getString("objectPrefix", "flumeData-");
  }
}
