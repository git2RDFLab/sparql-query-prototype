package de.leipzig.htwk.gitrdf.sparql.query.api.controller;

import de.leipzig.htwk.gitrdf.sparql.query.service.impl.SparqlQueryServiceImpl;
import de.leipzig.htwk.gitrdf.sparql.query.utils.LongUtils;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;

@RestController
@RequiredArgsConstructor
@Slf4j
public class QueryController {

    private final SparqlQueryServiceImpl sparqlQueryService;

    @PostMapping(
            value = "/api/v1/github/rdf/query/{id}",
            consumes = MediaType.TEXT_PLAIN_VALUE,
            produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public @ResponseBody Resource getResultOfQuery(
            @PathVariable("id") String id ,
            @RequestBody String query, HttpServletResponse httpServletResponse) throws SQLException, IOException {

        long longId = LongUtils.convertStringToLongIdOrThrowException(id);

        File tempRdfQueryResultJsonFile = sparqlQueryService.performSparqlQuery(longId, query);

        Resource responseResource
                = new InputStreamResource(new BufferedInputStream(new FileInputStream(tempRdfQueryResultJsonFile)));

        httpServletResponse.setHeader("Content-Disposition", "attachment; filename=\"query-result.json\"");

        return responseResource;
    }



}
