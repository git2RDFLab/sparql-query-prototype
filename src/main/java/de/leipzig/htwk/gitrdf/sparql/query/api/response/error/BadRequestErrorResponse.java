package de.leipzig.htwk.gitrdf.sparql.query.api.response.error;

import lombok.Value;

@Value
public class BadRequestErrorResponse {
    String status;
    String reason;
    String solution;
}
