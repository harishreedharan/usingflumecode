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

import com.google.protobuf.ByteString;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.EventSerializer;
import usingflume.ch03.UsingFlumeEvent;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

public class ProtobufSerializer implements EventSerializer {
  private final boolean writeHeaderAndFooter;
  private final BufferedOutputStream stream;
  private static final byte[] footer = ("End Using Flume protobuf " +
    "file").getBytes();
  private static final byte[] header = ("Begin Using Flume protobuf" +
    " file").getBytes();


  private ProtobufSerializer(Context ctx, OutputStream stream) {
    writeHeaderAndFooter = ctx.getBoolean("writeHeaderAndFooter",
      false);
    this.stream = new BufferedOutputStream(stream);
  }

  @Override
  public void afterCreate() throws IOException {
    if(writeHeaderAndFooter) {
      stream.write(header);
    }
  }

  @Override
  public void afterReopen() throws IOException {

  }

  @Override
  public void write(Event event) throws IOException {
    UsingFlumeEvent.Event.Builder
      builder = UsingFlumeEvent.Event.newBuilder();
    for (Map.Entry<String, String> entry : event.getHeaders()
      .entrySet()) {
      builder.addHeader(UsingFlumeEvent.Header.newBuilder()
        .setKey(entry.getKey())
        .setVal(entry.getValue()).build());
    }
    builder.setBody(ByteString.copyFrom(event.getBody()));
    UsingFlumeEvent.Event e = builder.build();
    stream.write(ByteBuffer.allocate(Integer.SIZE / 8).putInt(e
      .getSerializedSize()).array());
    e.writeTo(stream);
  }

  @Override
  public void flush() throws IOException {
    stream.flush();
  }

  @Override
  public void beforeClose() throws IOException {
    if (writeHeaderAndFooter) {
      stream.write(footer);
    }
  }

  @Override
  public boolean supportsReopen() {
    return false;
  }

  public static class Builder implements EventSerializer.Builder {

    @Override
    public EventSerializer build(Context context,
      OutputStream outputStream) {
      return new ProtobufSerializer(context, outputStream);
    }
  }
}
