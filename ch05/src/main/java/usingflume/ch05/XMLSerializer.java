package usingflume.ch05;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.EventSerializer;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * A class that serializes Flume events into XML in the format
 * specified in
 * XmlEventRepresentation.xml
 */

public class XMLSerializer implements EventSerializer {

  private final OutputStream hdfsStream;
  private final String CONF_BUFFER_SIZE = "bufferSize";
  private final int DEFAULT_BUFFER_SIZE = 2048;
  private final String CONF_CHARSET = "charset";
  private final String DEFAULT_CHARSET = "UTF-8";
  private final Charset charset;
  private final byte[] bodyStart, bodyEnd;
  private final byte[] headerStart, headerEnd;
  private final byte[] newLine;
  private final byte[] eventStart, eventEnd;

  private XMLSerializer(Context context, OutputStream stream) {
    int bufferSize = context.getInteger(CONF_BUFFER_SIZE,
      DEFAULT_BUFFER_SIZE);
    charset = Charset.forName(context.getString(CONF_CHARSET,
      DEFAULT_CHARSET));
    bodyStart = "<body>".getBytes(charset);
    bodyEnd = "</body>".getBytes(charset);
    headerStart = "<headers>".getBytes(charset);
    headerEnd = "</headers>".getBytes(charset);
    newLine = "\n".getBytes(charset);
    eventStart = "<event>".getBytes(charset);
    eventEnd = "</event>".getBytes(charset);
    this.hdfsStream = new BufferedOutputStream(stream, bufferSize);
  }

  @Override
  public void afterCreate() throws IOException {
    // Any file headers can be written in this method.
    // In this example, we write a simple header as a comment.
    hdfsStream.write(
      ("<events>").getBytes(charset));
    hdfsStream.write(newLine);
  }

  @Override
  public void afterReopen() throws IOException {
  }

  @Override
  public void write(Event event) throws IOException {
    hdfsStream.write(eventStart);
    hdfsStream.write(newLine);
    hdfsStream.write(headerStart);
    hdfsStream.write(newLine);
    // Ensure that the headers are sorted, so the test passes.
    Map<String, String> headers
      = new TreeMap<String, String>(event.getHeaders());
    for (Map.Entry<String, String> header : headers.entrySet()) {
      String key = header.getKey();
      hdfsStream.write(
        ("<" + key + ">" + header.getValue() + "</" + key +
          ">").getBytes(charset));
      hdfsStream.write(newLine);
    }
    hdfsStream.write(headerEnd);
    hdfsStream.write(newLine);
    hdfsStream.write(bodyStart);
    // Assumption: Event body is already in the required charset.
    hdfsStream.write(event.getBody());
    hdfsStream.write(bodyEnd);
    hdfsStream.write(newLine);
    hdfsStream.write(eventEnd);
    hdfsStream.write(newLine);
  }

  @Override
  public void flush() throws IOException {
    hdfsStream.flush();
  }

  @Override
  public void beforeClose() throws IOException {
    hdfsStream.write("</events>".getBytes(charset));
    flush();
  }

  @Override
  public boolean supportsReopen() {
    return false;
  }

  public static class Builder implements EventSerializer.Builder {

    @Override
    public EventSerializer build(Context context, OutputStream out) {
      return new XMLSerializer(context, out);
    }

  }
}