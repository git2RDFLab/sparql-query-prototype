package de.leipzig.htwk.gitrdf.sparql.query.api.exception;

import lombok.Getter;

@Getter
public class NotFoundException extends RuntimeException {

    public static NotFoundException githubEntryNotFound(long id) {

        String status = "Not found";
        String reason = String.format("No github to rdf entry found for id '%d'", id);
        String solution = "Provide an id for an existing github to rdf entry";

        String message = getMessageFrom(status, reason, solution);

        return new NotFoundException(message, status, reason, solution);
    }

    private final String status;
    private final String reason;
    private final String solution;

    private NotFoundException(String message, String status, String reason, String solution) {
        super(message);

        this.status = status;
        this.reason = reason;
        this.solution = solution;
    }

    private static String getMessageFrom(String status, String reason, String solution) {
        return String.format("Status: %s, Reason: %s, Solution: %s", status, reason, solution);
    }
}
