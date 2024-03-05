package de.leipzig.htwk.gitrdf.sparql.query.api.controller;

import de.leipzig.htwk.gitrdf.sparql.query.api.model.request.QueryRequest;
import de.leipzig.htwk.gitrdf.sparql.query.service.impl.SparqlQueryServiceImpl;
import de.leipzig.htwk.gitrdf.sparql.query.utils.LongUtils;
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
@RequestMapping(path = "/query-service/api/v1/github")
public class QueryController {

    // for more infos regarding a sparql conform api: https://www.w3.org/TR/sparql11-protocol/
    // https://www.w3.org/2001/sw/DataAccess/rq23/#ask

    private final SparqlQueryServiceImpl sparqlQueryService;

    @GetMapping(value = "/rdf/query/{id}", produces = "application/sparql-results+json")
    public @ResponseBody Resource getResultOfGetQuery(
            @PathVariable("id") String id,
            @RequestParam("query") String query) throws SQLException, IOException {

        return getQueryJsonResultResponseFrom(id, query);
    }

    @PostMapping(
            value = "/rdf/query/{id}",
            consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE,
            produces = "application/sparql-results+json")
    public @ResponseBody Resource getResultOfPostQueryFormEncoded(
            @PathVariable("id") String id,
            QueryRequest queryRequest) throws SQLException, IOException {

        return getQueryJsonResultResponseFrom(id, queryRequest.getQuery());
    }

    @PostMapping(
            value = "/rdf/query/{id}",
            consumes = "application/sparql-query",
            produces = "application/sparql-results+json")
    public @ResponseBody Resource getResultOfPostQueryDirectRequest(
            @PathVariable("id") String id,
            @RequestBody String query) throws SQLException, IOException {

        return getQueryJsonResultResponseFrom(id, query);
    }

    private Resource getQueryJsonResultResponseFrom(String entityId, String query) throws SQLException, IOException {

        long longId = LongUtils.convertStringToLongIdOrThrowException(entityId);

        File tempRdfQueryResultJsonFile = sparqlQueryService.performSparqlQuery(longId, query);

        return new InputStreamResource(new BufferedInputStream(new FileInputStream(tempRdfQueryResultJsonFile)));
    }

}
