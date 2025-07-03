package com.kinnarastudio.obclient.service;

import com.kinnarastudio.obclient.exceptions.RestClientException;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.*;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.logging.Logger;

public class RestService {
    public final static Logger logger = Logger.getLogger(RestService.class.getName());

    private static RestService instance = null;

    private boolean isDebug = false;

    private boolean ignoreCertificate = false;

    private final HttpClient client;

    private RestService() throws RestClientException {
        this.client = getHttpClient();
    }

    public static synchronized RestService getInstance() throws RestClientException {
        if (instance == null) instance = new RestService();
        return instance;
    }

    protected HttpClient getHttpClient() throws RestClientException {
        try {
            if (ignoreCertificate) {
                SSLContext sslContext = new SSLContextBuilder()
                        .loadTrustMaterial(null, (certificate, authType) -> true).build();
                return HttpClients.custom().setSSLContext(sslContext)
                        .setSSLHostnameVerifier(new NoopHostnameVerifier())
                        .build();
            } else {
                return HttpClientBuilder.create().build();
            }
        } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
            throw new RestClientException(e);
        }
    }

    public int getResponseStatus(@Nonnull HttpResponse response) throws RestClientException {
        return Optional.of(response)
                .map(HttpResponse::getStatusLine)
                .map(StatusLine::getStatusCode)
                .orElseThrow(() -> new RestClientException("Error getting status code"));
    }

    public boolean isJsonResponse(@Nonnull HttpResponse response) throws RestClientException {
        return getResponseContentType(response).contains("json");
    }

    public String getResponseContentType(@Nonnull HttpResponse response) throws RestClientException {
        return Optional.of(response)
                .map(HttpResponse::getEntity)
                .map(HttpEntity::getContentType)
                .map(Header::getValue)
                .orElseGet(() -> {
                    logger.warning("Empty header content-type");
                    return "";
                });
    }

    public int getStatusGroupCode(int status) {
        if (isDebug) {
            logger.info("getStatusGroupCode : status [" + status + "]");
        }

        return status - (status % 100);
    }

    public HttpResponse doGet(@Nonnull String url, String username, String password) throws RestClientException {
        try {
            final Map<String, String> headers = Collections.singletonMap("Authorization", getBasicAuthenticationHeader(username, password));
            final HttpUriRequest request = getHttpRequest(url, Method.GET, headers, null);
            return client.execute(request);
        } catch (IOException e) {
            throw new RestClientException(e);
        }
    }

    public HttpResponse doGet(@Nonnull String url, @Nonnull Map<String, String> headers) throws RestClientException {
        try {
            final HttpUriRequest request = getHttpRequest(url, Method.GET, headers, null);
            return client.execute(request);
        } catch (IOException e) {
            throw new RestClientException(e);
        }
    }

    public HttpResponse doPost(@Nonnull String url, String username, String password, @Nullable JSONObject bodyPayload) throws RestClientException {
        try {
            final Map<String, String> headers = Collections.singletonMap("Authorization", getBasicAuthenticationHeader(username, password));
            final HttpUriRequest request = getHttpRequest(url, Method.POST, headers, bodyPayload);
            return client.execute(request);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public HttpResponse doPost(@Nonnull String url, @Nonnull Map<String, String> headers, @Nullable JSONObject bodyPayload) throws RestClientException {
        try {
            final HttpUriRequest request = getHttpRequest(url, Method.POST, headers, bodyPayload);
            return client.execute(request);
        } catch (IOException e) {
            throw new RestClientException(e);
        }
    }

    public HttpResponse doDelete(@Nonnull String url, String username, String password) throws RestClientException {
        try {
            final Map<String, String> headers = Collections.singletonMap("Authorization", getBasicAuthenticationHeader(username, password));
            final HttpUriRequest request = getHttpRequest(url, Method.DELETE, headers, null);
            return client.execute(request);
        } catch (IOException e) {
            throw new RestClientException(e);
        }
    }

    public HttpResponse doDelete(@Nonnull String url, @Nonnull Map<String, String> headers) throws RestClientException {
        try {
            final HttpUriRequest request = getHttpRequest(url, Method.DELETE, headers, null);
            return client.execute(request);
        } catch (IOException e) {
            throw new RestClientException(e);
        }
    }

    protected HttpUriRequest getHttpRequest(@Nonnull String url, @Nonnull Method method, @Nonnull Map<String, String> headers, @Nullable JSONObject bodyPayload) throws RestClientException {
        if (isDebug) {
            logger.info("getHttpRequest : url [" + url + "] method [" + method + "] bodyPayload [" + bodyPayload + "]");
        }

        @Nullable HttpEntity httpEntity;
        if (method == Method.GET || method == Method.DELETE || bodyPayload == null) {
            httpEntity = null;
        } else {
            httpEntity = new StringEntity(bodyPayload.toString(), ContentType.APPLICATION_JSON);
        }

        final HttpRequestBase request;

        switch (method) {
            case GET:
                request = new HttpGet(url);
                break;
            case PUT:
                request = new HttpPut(url);
                break;
            case POST:
                request = new HttpPost(url);
                break;
            case DELETE:
                request = new HttpDelete(url);
                break;
            default:
                throw new RestClientException("Method [" + method + "] not supported");
        }

        headers.forEach(request::addHeader);

        if (httpEntity != null) {
            ((HttpEntityEnclosingRequestBase) request).setEntity(httpEntity);
        }

        return request;
    }

    public String getBasicAuthenticationHeader(String username, String password) {
        return "Basic " + Base64.getEncoder().encodeToString(String.format("%s:%s", username, password).getBytes());
    }

    public void setIgnoreCertificate(boolean ignoreCertificate) {
        this.ignoreCertificate = ignoreCertificate;
    }

    public void setDebug(boolean debug) {
        isDebug = debug;
    }

    public enum Method {
        GET,
        POST,
        PUT,
        DELETE,
    }
}
