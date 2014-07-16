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
package usingflume.ch03;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractEventDrivenSource;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class TransactionSource extends AbstractEventDrivenSource implements
  FlumeCreditCardAuth {

  private static final String CONF_HOST = "host";
  private static final String CONF_PORT = "port";
  private static final String DELIMITER = ";";

  private String host;
  private Integer port;
  private Server srv;

  @Override
  protected void doConfigure(Context context) throws FlumeException {
    host = context.getString(CONF_HOST);
    Preconditions.checkArgument(host != null && !host.isEmpty(),
      "Host must be specified");
    port = Preconditions.checkNotNull(context.getInteger(CONF_PORT),
      "Port must be specified");
  }

  @Override
  protected void doStart() throws FlumeException {
    srv = new NettyServer(
      new SpecificResponder(FlumeCreditCardAuth.class, this),
      new InetSocketAddress(host, port));
    srv.start();
    super.start();
  }

  @Override
  protected void doStop() throws FlumeException {
    srv.close();
    try {
      srv.join();
    } catch (InterruptedException e) {
      throw new FlumeException("Interrupted while waiting for Netty Server to" +
        " shutdown.", e);
    }
  }

  @Override
  public Status transactionsCompleted(List<CreditCardTransaction> transactions)
    throws AvroRemoteException {
    Status status;
    List<Event> events = new ArrayList<Event>(transactions.size());
    for (CreditCardTransaction txn : transactions) {
      StringBuilder builder = new StringBuilder();
      builder.append(txn.getCardNumber()).append(DELIMITER)
        .append(txn.getLocation()).append(DELIMITER)
        .append(txn.getAmount()).append(DELIMITER);
      events.add(EventBuilder.withBody(builder.toString().getBytes(
        Charsets.UTF_8)));
    }
    try {
      getChannelProcessor().processEventBatch(events);
      status = Status.OK;
    } catch (Exception e) {
      status = Status.FAILED;
    }
    return status;
  }
}
