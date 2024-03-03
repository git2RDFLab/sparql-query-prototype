package de.leipzig.htwk.gitrdf.sparql.query.api.advice;

import de.leipzig.htwk.gitrdf.sparql.query.api.exception.BadRequestException;
import de.leipzig.htwk.gitrdf.sparql.query.api.response.error.BadRequestErrorResponse;
import de.leipzig.htwk.gitrdf.sparql.query.api.response.error.InternalServerErrorResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import java.io.IOException;
import java.sql.SQLException;

@ControllerAdvice
@Slf4j
public class GeneralControllerAdvice {


    @ExceptionHandler(value = BadRequestException.class)
    public ResponseEntity<BadRequestErrorResponse> handleBadRequestException(BadRequestException ex) {

        log.info("Bad request exception during request handling.", ex);

        BadRequestErrorResponse response
                = new BadRequestErrorResponse(ex.getStatus(), ex.getReason(), ex.getSolution());

        return new ResponseEntity<>(response, HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler(value = IOException.class)
    public ResponseEntity<InternalServerErrorResponse> handleIOException(IOException ex) {

        log.warn("IOException during request handling.", ex);

        return new ResponseEntity<>(
                InternalServerErrorResponse.unexpectedException(),
                HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @ExceptionHandler(value = SQLException.class)
    public ResponseEntity<InternalServerErrorResponse> handleSqlException(SQLException ex) {

        log.warn("SqlException during request handling.", ex);

        return new ResponseEntity<>(
                InternalServerErrorResponse.unexpectedException(),
                HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @ExceptionHandler(value = RuntimeException.class)
    public ResponseEntity<InternalServerErrorResponse> handleRuntimeException(RuntimeException ex) {

        log.warn("RuntimeException during request handling.", ex);

        return new ResponseEntity<>(
                InternalServerErrorResponse.unexpectedException(),
                HttpStatus.INTERNAL_SERVER_ERROR);
    }

}
