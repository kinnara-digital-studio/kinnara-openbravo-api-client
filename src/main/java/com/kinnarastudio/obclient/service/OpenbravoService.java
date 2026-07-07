package com.kinnarastudio.obclient.service;

import com.kinnarastudio.commons.Try;
import com.kinnarastudio.commons.jsonstream.JSONCollectors;
import com.kinnarastudio.commons.jsonstream.JSONStream;
import com.kinnarastudio.commons.jsonstream.model.JSONObjectEntry;
import com.kinnarastudio.obclient.annotation.ObEntity;
import com.kinnarastudio.obclient.annotation.ObField;
import com.kinnarastudio.obclient.exceptions.OpenbravoClientException;
import com.kinnarastudio.obclient.exceptions.OpenbravoCreateRecordException;
import com.kinnarastudio.obclient.exceptions.RestClientException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class OpenbravoService {
    public final static DateFormat DF = new SimpleDateFormat("yyyy-MM-dd");
    public final static Logger logger = Logger.getLogger(OpenbravoService.class.getName());
    private final String username;
    private final String password;
    private final String baseUrl;
    private final boolean ignoreCertificateError;
    private final boolean shortCircuit;
    Exception cutCircuitCause = null;
    private boolean cutCircuit;

    public OpenbravoService(String baseUrl, String username, String password) {
        this.baseUrl = baseUrl;
        this.username = username;
        this.password = password;
        this.shortCircuit = false;
        this.cutCircuit = false;
        this.ignoreCertificateError = false;
    }

    public OpenbravoService(String baseUrl, String username, String password, boolean ignoreCertificateError, boolean shortCircuit) {
        this.baseUrl = baseUrl;
        this.username = username;
        this.password = password;
        this.shortCircuit = shortCircuit;
        this.cutCircuit = false;
        this.ignoreCertificateError = ignoreCertificateError;
    }

    public Map<String, Object> delete(@Nonnull String tableEntity, @Nonnull String recordId) throws OpenbravoClientException {
        return delete(tableEntity, recordId, false);
    }

    public Map<String, Object> delete(@Nonnull String tableEntity, @Nonnull String recordId, boolean noFilterActive) throws OpenbravoClientException {
        try (RestService restService = new RestService()) {

            restService.setIgnoreCertificate(ignoreCertificateError);

            final StringBuilder url = new StringBuilder()
                    .append(baseUrl)
                    .append("/org.openbravo.service.json.jsonrest/")
                    .append(tableEntity)
                    .append("/")
                    .append(recordId);

            if (noFilterActive) {
                addUrlParameter(url, "_noActiveFilter", "true");
            }

            final Map<String, String> headers = Collections.singletonMap("Authorization", restService.getBasicAuthenticationHeader(username, password));
            try (CloseableHttpResponse response = restService.doDelete(url.toString(), headers)) {

                final int statusCode = restService.getResponseStatus(response);
                if (restService.getStatusGroupCode(statusCode) != 200) {
                    throw new RestClientException("Response code [" + statusCode + "] is not 200 (Success) url [" + url + "]");
                } else if (statusCode != 200) {
                    logger.warning("Response code [" + statusCode + "] is considered as success");
                }

                if (!restService.isJsonResponse(response)) {
                    throw new RestClientException("Content type is not JSON");
                }

                try (BufferedReader br = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))) {
                    final String responsePayload = br.lines().collect(Collectors.joining());

                    final JSONObject jsonResponse = new JSONObject(responsePayload)
                            .getJSONObject("response");


                    final int status = jsonResponse.optInt("status", -1);
                    if (status != 0) {
                        throw new OpenbravoClientException(responsePayload);
                    }

                    final JSONObject jsonData = jsonResponse.getJSONObject("data");
                    return JSONStream.of(jsonData, Try.onBiFunction(JSONObject::getString))
                            .collect(Collectors.toUnmodifiableMap(JSONObjectEntry::getKey, JSONObjectEntry::getValue));
                }
            }
        } catch (RestClientException | JSONException | IOException e) {
            throw new OpenbravoClientException(e);
        }
    }

    public <T> Optional<T> get(Class<T> clazz, @Nonnull String primaryKey) throws OpenbravoClientException {
        return Arrays.stream(get(clazz, Collections.singletonMap("id", primaryKey)))
                .map(o -> (T) o)
                .findFirst();
    }

    @Nonnull
    public Map<String, Object> get(@Nonnull String tableEntity, @Nonnull String recordId) throws OpenbravoClientException {
        return get(tableEntity, recordId, false);
    }

    @Nonnull
    public Map<String, Object> get(@Nonnull String tableEntity, @Nonnull String recordId, boolean noFilterActive) throws OpenbravoClientException {
        try (RestService restService = new RestService()) {

            restService.setIgnoreCertificate(ignoreCertificateError);

            final StringBuilder url = new StringBuilder()
                    .append(baseUrl)
                    .append("/org.openbravo.service.json.jsonrest/")
                    .append(tableEntity)
                    .append("/")
                    .append(recordId);

            if (noFilterActive) {
                addUrlParameter(url, "_noActiveFilter", "true");
            }

            final Map<String, String> headers = Collections.singletonMap("Authorization", restService.getBasicAuthenticationHeader(username, password));
            try (CloseableHttpResponse response = restService.doGet(url.toString(), headers)) {

                final int statusCode = restService.getResponseStatus(response);
                if (restService.getStatusGroupCode(statusCode) != 200) {
                    throw new RestClientException("Response code [" + statusCode + "] is not 200 (Success) url [" + url + "]");
                } else if (statusCode != 200) {
                    logger.warning("Response code [" + statusCode + "] is considered as success");
                }

                if (!restService.isJsonResponse(response)) {
                    throw new RestClientException("Content type is not JSON");
                }

                try (BufferedReader br = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))) {
                    final String responsePayload = br.lines().collect(Collectors.joining());

                    final JSONObject jsonResponse = new JSONObject(responsePayload)
                            .getJSONObject("response");


                    final int status = jsonResponse.optInt("status", -1);
                    if (status != 0) {
                        throw new OpenbravoClientException(responsePayload);
                    }

                    final JSONObject jsonData = jsonResponse.getJSONObject("data");
                    return JSONStream.of(jsonData, Try.onBiFunction(JSONObject::getString))
                            .collect(Collectors.toUnmodifiableMap(JSONObjectEntry::getKey, JSONObjectEntry::getValue));
                }
            }
        } catch (RestClientException | JSONException | IOException e) {
            throw new OpenbravoClientException(e);
        }
    }

    public <T> Object[] get(Class<T> clazz, Map<String, String> filter) throws OpenbravoClientException {
        final String where = getFilterWhereCondition(filter);
        return get(clazz, where, null, null, null, null, null);
    }

    public Map<String, Object>[] get(@Nonnull String tableEntity, Map<String, String> filter) throws OpenbravoClientException {
        final String where = getFilterWhereCondition(filter);
        return get(tableEntity, null, where, null, null, null, null, null);
    }

    /**
     * @param clazz
     * @param condition
     * @param arguments
     * @param sort
     * @param desc
     * @param startRow
     * @param endRow
     * @param <T>
     * @return arrays of T[]
     * @throws OpenbravoClientException
     */
    public <T> Object[] get(@Nonnull Class<T> clazz, @Nullable String condition, Object[] arguments, @Nullable String sort, @Nullable Boolean desc, @Nullable Integer startRow, @Nullable Integer endRow) throws OpenbravoClientException {
        final String tableEntity = getTableEntity(clazz);
        final String[] fields = getFields(clazz);
        final Map<String, Object>[] records = get(tableEntity, fields, condition, arguments, sort, desc, startRow, endRow);

        return Arrays.stream(records)
                .map(Try.onFunction(m -> {
                    try {
                        return clazz.getConstructor(Map.class).newInstance(m);
                    } catch (NoSuchMethodException ignored) {
                        final T instance = clazz.getConstructor().newInstance();

                        Optional.of(clazz)
                                .map(Class::getDeclaredFields)
                                .stream()
                                .flatMap(Arrays::stream)
                                .forEach(Try.onConsumer(field -> {
                                    final String jsonKey = Optional.of(field)
                                            .map(f -> f.getAnnotation(ObField.class))
                                            .map(ObField::value)
                                            .orElseGet(field::getName);
                                    final String classAttribute = field.getName();
                                    final String setterName = "set" + classAttribute.substring(0, 1).toUpperCase() + classAttribute.substring(1);
                                    final Object value = m.get(jsonKey);
                                    if (value == null) return;

                                    try {
                                        clazz.getDeclaredMethod(setterName, value.getClass()).invoke(instance, value);
                                    } catch (NoSuchMethodException e) {
                                        logger.log(Level.SEVERE, e.getMessage(), e);
                                        logger.log(Level.WARNING, "Trying to call pass value [" + value + "] as [" + String.class.getName() + "] instead of [" + value.getClass().getName() + "]");
                                        clazz.getDeclaredMethod(setterName, String.class).invoke(instance, String.valueOf(value));
                                    }
                                }));

                        return instance;
                    }
                }))
                .toArray();
    }

    public Map<String, Object>[] get(@Nonnull String tableEntity, @Nullable String[] fields, @Nullable String condition, Object[] arguments, @Nullable String sort, @Nullable Boolean desc, @Nullable Integer startRow, @Nullable Integer endRow) throws OpenbravoClientException {
        return get(tableEntity, fields, condition, arguments, sort, desc, startRow, endRow, false);
    }

    /**
     * @param tableEntity
     * @param fields
     * @param condition
     * @param arguments
     * @param sort
     * @param desc
     * @param startRow
     * @param endRow
     * @return
     * @throws OpenbravoClientException
     */
    public Map<String, Object>[] get(@Nonnull String tableEntity, @Nullable String[] fields, @Nullable String condition, Object[] arguments, @Nullable String sort, @Nullable Boolean desc, @Nullable Integer startRow, @Nullable Integer endRow, boolean noFilterActive) throws OpenbravoClientException {
        logger.info("get : baseUrl [" + baseUrl + "] tableEntity [" + tableEntity + "] username [" + username + "]");

        try (RestService restService = new RestService()) {

            restService.setIgnoreCertificate(ignoreCertificateError);

            final StringBuilder url = new StringBuilder()
                    .append(baseUrl)
                    .append("/org.openbravo.service.json.jsonrest/")
                    .append(tableEntity);

            if (fields != null && fields.length > 0) {
                addUrlParameter(url, "_selectedProperties", String.join(",", fields));
            }

            if (noFilterActive) {
                addUrlParameter(url, "_noActiveFilter", "true");
            }

            if (startRow != null) {
                addUrlParameter(url, "_startRow", startRow.toString());
            }

            if (endRow != null) {
                addUrlParameter(url, "_endRow", endRow.toString());
            }

            if (condition != null && !condition.isEmpty()) {
                final String where = arguments == null ? condition : formatArguments(condition, arguments);
                addUrlParameter(url, "_where", URLEncoder.encode(where));
            }

            if (sort != null && !sort.isEmpty()) {
                if (desc != null && desc) {
                    sort += " desc";
                }
                addUrlParameter(url, "_orderBy", URLEncoder.encode(sort.replaceAll("\\$", ".")));
            }

            final Map<String, String> headers = Collections.singletonMap("Authorization", restService.getBasicAuthenticationHeader(username, password));
            try (CloseableHttpResponse response = restService.doGet(url.toString(), headers)) {

                try (BufferedReader br = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))) {
                    final String responsePayload = br.lines().collect(Collectors.joining());

                    final int statusCode = restService.getResponseStatus(response);
                    if (restService.getStatusGroupCode(statusCode) != 200) {
                        throw new RestClientException("Response code [" + statusCode + "] is not 200 (Success) url [" + url + "]");
                    } else if (statusCode != 200) {
                        logger.warning("Response code [" + statusCode + "] is considered as success");
                    }

                    if (!restService.isJsonResponse(response)) {
                        throw new RestClientException("Content type is not JSON");
                    }

                    final JSONObject jsonResponse = new JSONObject(responsePayload)
                            .getJSONObject("response");

                    final int status = jsonResponse.optInt("status", -1);
                    if (status != 0) {
                        throw new OpenbravoClientException(responsePayload);
                    }

                    final JSONArray jsonData = jsonResponse.getJSONArray("data");
                    return JSONStream.of(jsonData, Try.onBiFunction(JSONArray::getJSONObject))
                            .map(json -> JSONStream.of(json, Try.onBiFunction(JSONObject::get))
                                    .collect(Collectors.toMap(JSONObjectEntry::getKey, JSONObjectEntry::getValue)))
                            .toArray(Map[]::new);
                }
            }
        } catch (RestClientException | JSONException | IOException e) {
            throw new OpenbravoClientException(e);
        }
    }

    protected String formatArguments(String condition, Object[] arguments) {
        final Pattern p = Pattern.compile("\\?");
        final Matcher m = p.matcher(condition);

        final StringBuilder sb = new StringBuilder();
        final List<Object> args = new ArrayList<>();
        if (arguments != null) {
            for (int i = 0; i < arguments.length && m.find(); i++) {
                final Object argument = arguments[i];

                final String replacement;
                if (argument instanceof Integer || argument instanceof Long) {
                    replacement = "%d";
                    args.add(argument);
                } else if (argument instanceof Float || argument instanceof Double) {
                    replacement = "%.2f";
                    args.add(argument);
                } else if (argument instanceof Date) {
                    replacement = "'%s'";
                    args.add(DF.format(argument));
                } else {
                    replacement = "'%s'";
                    args.add(String.valueOf(argument).replaceAll(",", "''"));
                }

                m.appendReplacement(sb, replacement);
            }
        }

        m.appendTail(sb);

        return String.format(sb.toString(), args.toArray(new Object[0]));
    }

    public synchronized Map<String, Object>[] post(@Nonnull String tableEntity, @Nonnull Map<String, Object>[] rows) throws OpenbravoClientException {
        try (RestService restService = new RestService()) {

            restService.setIgnoreCertificate(ignoreCertificateError);

            final StringBuilder url = new StringBuilder().append(baseUrl).append("/org.openbravo.service.json.jsonrest/").append(tableEntity);
            final Map<String, String> headers = Collections.singletonMap("Authorization", restService.getBasicAuthenticationHeader(username, password));

            cutCircuit = false;

            final Map[] result = Arrays.stream(rows)
                    .map((Map<String, Object> row) -> {
                        if (cutCircuit) return null;
                        try {
                            final JSONObject jsonBody = new JSONObject() {{
                                put("data", row.entrySet()
                                        .stream()
                                        .collect(JSONCollectors.toJSONObject(Map.Entry::getKey, Map.Entry::getValue)));
                            }};

                            try (CloseableHttpResponse response = restService.doPost(url.toString(), headers, jsonBody)) {

                                final int statusCode = restService.getResponseStatus(response);
                                if (restService.getStatusGroupCode(statusCode) != 200) {
                                    throw new RestClientException("Response code [" + statusCode + "] is not 200 (Success) url [" + url + "]");
                                } else if (statusCode != 200) {
                                    logger.warning("Response code [" + statusCode + "] is considered as success");
                                }

                                if (!restService.isJsonResponse(response)) {
                                    throw new RestClientException("Content type is not JSON");
                                }

                                try (BufferedReader br = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))) {
                                    final String responsePayload = br.lines().collect(Collectors.joining());
                                    final JSONObject jsonResponse = new JSONObject(responsePayload)
                                            .getJSONObject("response");

                                    final int status = jsonResponse.getInt("status");
                                    if (status != 0) {
                                        if (status == -4) {
                                            final JSONObject jsonErrors = jsonResponse.getJSONObject("errors");
                                            final Map<String, String> errors = JSONStream.of(jsonErrors, Try.onBiFunction(JSONObject::getString))
                                                    .collect(Collectors.toUnmodifiableMap(JSONObjectEntry::getKey, JSONObjectEntry::getValue));
                                            throw new OpenbravoCreateRecordException(errors);
                                        } else if (status == -1) {
                                            throw new OpenbravoClientException(jsonResponse.getJSONObject("error").getString("message"));
                                        } else {
                                            throw new OpenbravoClientException(responsePayload);
                                        }
                                    }

                                    final JSONArray jsonData = jsonResponse.getJSONArray("data");
                                    final Map<String, Object> data = JSONStream.of(jsonData, Try.onBiFunction(JSONArray::getJSONObject))
                                            .findFirst()
                                            .stream()
                                            .flatMap(json -> JSONStream.of(json, Try.onBiFunction(JSONObject::get)))
                                            .collect(Collectors.toUnmodifiableMap(JSONObjectEntry::getKey, JSONObjectEntry::getValue));

                                    return data;
                                }
                            }
                        } catch (OpenbravoClientException | RestClientException | IOException | JSONException |
                                 OpenbravoCreateRecordException e) {

                            logger.warning(e.getMessage());

                            if (shortCircuit) {
                                cutCircuit = true;
                                cutCircuitCause = e;
                                return null;
                            }

                            return Collections.<String, Object>emptyMap();
                        }
                    })
                    .filter(Objects::nonNull)
                    .toArray(Map[]::new);

            if (cutCircuit) {
                throw cutCircuitCause instanceof OpenbravoClientException
                        ? (OpenbravoClientException) cutCircuitCause
                        : new OpenbravoClientException(cutCircuitCause);
            }

            if (rows.length != result.length)
                throw new OpenbravoClientException("Request length [" + rows.length + "] and response length [" + result.length + "] are different");

            return (Map<String, Object>[]) result;
        } catch (RestClientException | IOException e) {
            throw new OpenbravoClientException(e);
        }
    }

    protected void addUrlParameter(@Nonnull final StringBuilder url, String parameterName, String parameterValue) {
        url.append(String.format("%s%s=%s", (url.toString().contains("?") ? "&" : "?"), parameterName, parameterValue));
    }

    protected String getFilterWhereCondition(Map<String, String> filter) {
        return Optional.ofNullable(filter)
                .map(Map::entrySet)
                .stream()
                .flatMap(Collection::stream)
                .map(e -> e.getKey() + "='" + e.getValue() + "'")
                .collect(Collectors.joining(") AND (", "(", ")"));

    }

    protected void errorHandler(JSONObject jsonResponse) throws OpenbravoClientException, JSONException {
        int status = jsonResponse.getInt("status");
        if (status == -1) {
            throw new OpenbravoClientException("");
        }
    }

    protected String getTableEntity(Class<?> clazz) {
        return Optional.of(clazz)
                .map(c -> c.getAnnotation(ObEntity.class))
                .map(ObEntity::value)
                .orElseGet(clazz::getSimpleName);
    }

    protected String[] getFields(Class<?> clazz) {
        return Optional.of(clazz)
                .stream()
                .map(Class::getDeclaredFields)
                .flatMap(Arrays::stream)
                .map(field -> Optional.of(field)
                        .map(f -> f.getAnnotation(ObField.class))
                        .map(ObField::value)
                        .orElseGet(field::getName))
                .map(s -> s.replaceAll("\\$.*$", ""))
                .toArray(String[]::new);
    }
}
