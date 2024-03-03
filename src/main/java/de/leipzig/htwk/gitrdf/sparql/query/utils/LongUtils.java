package de.leipzig.htwk.gitrdf.sparql.query.utils;

import de.leipzig.htwk.gitrdf.sparql.query.api.exception.BadRequestException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class LongUtils {

    public static long convertStringToLongIdOrThrowException(String longId) {

        try {
            return Long.parseLong(longId, 10);
        } catch (NumberFormatException ex) {
            log.info("Couldn't convert string to long id. Exception is '{}'", ex, ex);
            throw BadRequestException.invalidId(longId);
        }
    }

}
