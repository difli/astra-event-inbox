package com.tds.inbox.service;

import com.tds.inbox.domain.SlupEvent;

/**
 * Parses a raw CloudEvent JSON string into a {@link SlupEvent}.
 * Extracting an interface makes the implementation mockable without
 * Byte Buddy inline instrumentation (which has Java 25 limitations).
 */
public interface EventParser {

    /**
     * @param json raw CloudEvent JSON
     * @return parsed {@link SlupEvent}
     * @throws CloudEventParseException if the message is malformed or missing required fields
     */
    SlupEvent parse(String json);
}
