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
package usingflume.ch07;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Parser;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.flume.Event;
import org.apache.flume.agent.embedded.EmbeddedAgent;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UsingFlumeEmbeddedAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger
    (UsingFlumeEmbeddedAgent.class);
  private final EmbeddedAgent agent = new EmbeddedAgent(
    "UsingFlume");
  private int batchSize = 100;

  public static void main(String args[]) throws Exception {
    UsingFlumeEmbeddedAgent usingFlumeEmbeddedAgent = new
      UsingFlumeEmbeddedAgent();
    usingFlumeEmbeddedAgent.run(args);
    int i = 0;
    while (i++ < 100) {
      usingFlumeEmbeddedAgent.generateAndSend();
    }
  }

  public void run(String args[]) throws Exception {
    Options opts = new Options();

    Option opt = new Option("r", "remote", true,
      "Remote host to connect " +
        "to");
    opt.setRequired(true);
    opts.addOption(opt);

    opt = new Option("p", "port", true, "Port to connect to");
    opt.setRequired(true);
    opts.addOption(opt);

    opt = new Option("b", "batchSize", true, "Batch Size to use");
    opt.setRequired(false);
    opts.addOption(opt);

    Parser parser = new GnuParser();
    CommandLine commandLine = parser.parse(opts, args);

    if (commandLine.hasOption("h")) {
      new HelpFormatter().printHelp("UsingFlumeEmbeddedAgent", opts,
        true);
      return;
    }

    Map<String, String> config = new HashMap<String, String>();
    parseHostsAndPort(commandLine, config);
    config.put("source.type", "embedded");
    File dcDir = Files.createTempDir();
    dcDir.deleteOnExit();
    config.put("channel.type", "file");
    config.put("channel.capacity", "100000");
    config.put("channel.dataDirs", dcDir.toString() + "/data");
    config.put("channel.checkpointDir", dcDir.toString() + "/checkpoint");
    agent.configure(config);
    agent.start();
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        agent.stop();
      }
    }));
  }

  private void generateAndSend() {
    List<Event> events = new ArrayList<Event>(100);
    for (int i = 0; i < batchSize; i++) {
      events.add(EventBuilder.withBody(
        RandomStringUtils.randomAlphanumeric(1024).getBytes()));
    }
    try {
      agent.putAll(events);
    } catch (Throwable e) {
      LOGGER.error(
        "Error while attempting to write data to remote host at " +
          "%s:%s. Events will be dropped!");
      // The client cannot be reused, since we don't know why the
      // connection
      // failed. Destroy this client and create a new one.
    }
  }

  private void parseHostsAndPort(CommandLine commandLine,
    Map<String, String> config) {
    String host = commandLine.getOptionValue("r").trim();
    Preconditions.checkNotNull(host, "Remote host cannot be null.");

    String port = commandLine.getOptionValue("p").trim();
    Preconditions.checkNotNull(port, "Port cannot be null.");

    String[] hostnames = host.split(",");
    int hostCount = hostnames.length;
    final String sinkStr = "sink";
    StringBuilder stringNamesBuilder = new StringBuilder("");
    for (int i = 0; i < hostCount; i++) {
      stringNamesBuilder.append(sinkStr).append(i).append(" ");
    }
    // this puts sinks = sink0 sink1 sink2 sink 3 etc...
    config.put("sinks", stringNamesBuilder.toString());
    final String parameters[] = {"type", "hostname", "port",
                                 "batch-size"};
    final String avro = "avro";
    for (int i = 0; i < hostCount; i++) {
      final String currentSinkPrefix = sinkStr + String.valueOf(i) +
        ".";
      config.put(currentSinkPrefix + parameters[0], avro);
      config.put(currentSinkPrefix + parameters[1], hostnames[i]);
      config.put(currentSinkPrefix + parameters[2], port);
      config.put(currentSinkPrefix + parameters[3],
        String.valueOf(batchSize));
    }

    if (hostnames.length > 1) {
      config.put("processor.type", "load_balance");
      config.put("processor.backoff", "true");
      config.put("processor.selector", "round_robin");
      config.put("processor.selector.maxTimeout", "30000");
    } else {
      config.put("processor.type", "default");
    }
  }
}
