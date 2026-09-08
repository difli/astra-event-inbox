package com.tds.inbox.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * CloudEvents 1.0 envelope as produced by the upstream broker (Tardis Horizon).
 * Extra fields are silently ignored so the model stays forward-compatible.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CloudEvent(
        @JsonProperty("specversion")  String specVersion,
        @JsonProperty("id")           String id,
        @JsonProperty("source")       String source,
        @JsonProperty("type")         String type,
        @JsonProperty("time")         Instant time,
        @JsonProperty("datacontenttype") String dataContentType,
        @JsonProperty("data")         SlupData data
) {

    /**
     * Inner {@code data} payload of a SLUP CloudEvent message.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SlupData(
            @JsonProperty("guid")          String guid,
            @JsonProperty("operation")     String operation,
            @JsonProperty("requestName")   String requestName,
            @JsonProperty("applicationId") String applicationId,
            @JsonProperty("timestamp")     Instant timestamp,
            @JsonProperty("slupTimestamp") Long slupTimestamp,
            @JsonProperty("clientAddress") String clientAddress,
            @JsonProperty("fields")        List<Map<String, Object>> fields
    ) {}
}
