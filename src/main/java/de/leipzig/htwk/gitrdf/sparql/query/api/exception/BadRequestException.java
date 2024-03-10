package de.leipzig.htwk.gitrdf.sparql.query.api.exception;

import lombok.Getter;

@Getter
public class BadRequestException extends RuntimeException {

    public static BadRequestException invalidId(String idString) {

        String status = "Bad Request";
        String reason = String.format("Invalid id '%s' was given", idString);
        String solution = "Provide a valid id. Example id: 55";

        String message = getMessageFrom(status, reason, solution);

        return new BadRequestException(message, status, reason, solution);
    }

    public static BadRequestException emptySparqlQueryString() {

        String status = "Bad Request";
        String reason = "Empty sparql-query given";
        String solution = "Provide a non empty and valid sparql-query";

        String message = getMessageFrom(status, reason, solution);

        return new BadRequestException(message, status, reason, solution);
    }

    public static BadRequestException githubToRdfConversionNotDone(long id) {

        String status = "Bad Request";
        String reason = String.format("The status of the github to rdf entry with the id '%d' is not yet 'DONE'. The rdf was not yet produced.", id);
        String solution = "Provide an id for an github to rdf entry were the processing is already finished or wait till the processing for this particular entry is done";

        String message = getMessageFrom(status, reason, solution);

        return new BadRequestException(message, status, reason, solution);
    }

    private final String status;
    private final String reason;
    private final String solution;

    private BadRequestException(String message, String status, String reason, String solution) {
        super(message);

        this.status = status;
        this.reason = reason;
        this.solution = solution;
    }

    private static String getMessageFrom(String status, String reason, String solution) {
        return String.format("Status: %s, Reason: %s, Solution: %s", status, reason, solution);
    }
}
