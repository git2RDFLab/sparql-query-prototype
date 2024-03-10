package de.leipzig.htwk.gitrdf.sparql.query.api.response.error;

import lombok.Value;

@Value
public class NotFoundErrorResponse {
    String status;
    String reason;
    String solution;
}
