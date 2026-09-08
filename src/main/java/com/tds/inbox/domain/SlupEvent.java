package com.tds.inbox.domain;

import java.time.Instant;

/**
 * Normalised representation of an inbound SLUP event, ready for persistence.
 *
 * @param windowBucket   Processing-time window bucket (epoch seconds, floored to W).
 * @param eventTs        Event timestamp (ordering key).
 * @param eventId        Stable dedup key — identical across redeliveries.
 * @param guid           Subscriber GUID (observability).
 * @param operation      NEW | SET | DELETE
 * @param requestName    e.g. UTS
 * @param applicationId  Originating application.
 * @param clientAddress  Client IP as text.
 * @param ingestTime     Server clock at reception.
 * @param payload        Raw CloudEvent JSON string.
 */
public record SlupEvent(
        long    windowBucket,
        Instant eventTs,
        String  eventId,
        String  guid,
        String  operation,
        String  requestName,
        String  applicationId,
        String  clientAddress,
        Instant ingestTime,
        String  payload
) {}
