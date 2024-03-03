package de.leipzig.htwk.gitrdf.sparql.query.api.response.error;

import lombok.Value;

@Value
public class InternalServerErrorResponse {

    public static InternalServerErrorResponse unexpectedException() {
        return new InternalServerErrorResponse(
                "Internal Server Error",
                "An unexpected exception occurred",
                "Please try again later");
    }

    String status;
    String reason;
    String solution;

}
