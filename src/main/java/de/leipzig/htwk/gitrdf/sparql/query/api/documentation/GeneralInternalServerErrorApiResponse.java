package de.leipzig.htwk.gitrdf.sparql.query.api.documentation;

import de.leipzig.htwk.gitrdf.sparql.query.api.response.error.InternalServerErrorResponse;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import org.springframework.http.MediaType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@ApiResponse(
        responseCode = "500",
        description = "Internal Server Error",
        content = @Content(
                mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(implementation = InternalServerErrorResponse.class),
                examples = @ExampleObject("{\"status\": \"Internal Server Error\", \"reason\": \"An unexpected exception occurred\", \"solution\": \"Please try again later\"}")))
public @interface GeneralInternalServerErrorApiResponse {
}
