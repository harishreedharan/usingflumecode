package usingflume.ch03;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletInputStream;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.security.Principal;
import java.util.Enumeration;
import java.util.Locale;
import java.util.Map;

public class UsingFlumeServletRequest implements HttpServletRequest {
  private final InputStream inputStream = getClass().getClassLoader()
    .getResourceAsStream("XmlEventRepresentation.xml");

  @Override
  public String getAuthType() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public Cookie[] getCookies() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public long getDateHeader(String name) {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public String getHeader(String name) {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public Enumeration getHeaders(String name) {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public Enumeration getHeaderNames() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public int getIntHeader(String name) {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public String getMethod() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public String getPathInfo() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public String getPathTranslated() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public String getContextPath() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public String getQueryString() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public String getRemoteUser() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public boolean isUserInRole(String role) {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public Principal getUserPrincipal() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public String getRequestedSessionId() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public String getRequestURI() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public StringBuffer getRequestURL() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public String getServletPath() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public HttpSession getSession(boolean create) {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public HttpSession getSession() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public boolean isRequestedSessionIdValid() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public boolean isRequestedSessionIdFromCookie() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public boolean isRequestedSessionIdFromURL() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public boolean isRequestedSessionIdFromUrl() {
    throw new UnsupportedOperationException("Not supported.");
  }


  @Override
  public Object getAttribute(String name) {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public Enumeration getAttributeNames() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public String getCharacterEncoding() {
    return "UTF-8";
  }

  @Override
  public void setCharacterEncoding(String env)
    throws UnsupportedEncodingException {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public int getContentLength() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public String getContentType() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public ServletInputStream getInputStream() throws IOException {
    return new ServletInputStream() {
      @Override
      public int read() throws IOException {
        return inputStream.read();
      }
    };
  }

  @Override
  public String getParameter(String name) {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public Enumeration getParameterNames() {
    return null;
  }

  @Override
  public String[] getParameterValues(String name) {
    throw new UnsupportedOperationException("Not supported.");
  }


  @Override
  public Map getParameterMap() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public String getProtocol() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public String getScheme() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public String getServerName() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public int getServerPort() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public BufferedReader getReader() throws IOException {
    return new BufferedReader(new InputStreamReader(inputStream));
  }

  @Override
  public String getRemoteAddr() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public String getRemoteHost() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public void setAttribute(String name, Object o) {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public void removeAttribute(String name) {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public Locale getLocale() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public Enumeration getLocales() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public boolean isSecure() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public RequestDispatcher getRequestDispatcher(String path) {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public String getRealPath(String path) {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public int getRemotePort() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public String getLocalName() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public String getLocalAddr() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public int getLocalPort() {
    throw new UnsupportedOperationException("Not supported.");
  }
}
