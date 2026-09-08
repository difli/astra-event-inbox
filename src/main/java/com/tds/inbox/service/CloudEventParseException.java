package com.tds.inbox.service;

/**
 * Thrown when a Kafka message cannot be parsed as a valid CloudEvent.
 * Messages that cause this exception are routed to the dead-letter topic.
 */
public class CloudEventParseException extends RuntimeException {

    public CloudEventParseException(String message) {
        super(message);
    }

    public CloudEventParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
