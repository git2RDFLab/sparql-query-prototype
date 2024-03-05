package de.leipzig.htwk.gitrdf.sparql.query.service.impl;

import de.leipzig.htwk.gitrdf.database.common.entity.GithubRepositoryOrderEntity;
import de.leipzig.htwk.gitrdf.database.common.entity.enums.GitRepositoryOrderStatus;
import de.leipzig.htwk.gitrdf.database.common.entity.lob.GithubRepositoryOrderEntityLobs;
import de.leipzig.htwk.gitrdf.sparql.query.api.exception.BadRequestException;
import jakarta.persistence.EntityManager;
import lombok.RequiredArgsConstructor;
import org.apache.commons.io.FileUtils;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.*;
import java.sql.SQLException;

@Service
@RequiredArgsConstructor
public class SparqlQueryServiceImpl {

    private final int FIFTY_MEGABYTE = 1024 * 1024 * 50;

    private final int BUFFER_SIZE = FIFTY_MEGABYTE;

    private final String TURTLE_FORMAT = "TURTLE";

    private final EntityManager entityManager;

    @Transactional(rollbackFor = {SQLException.class, IOException.class}) // Runtime-Exceptions are rollbacked by default; Checked-Exceptions not
    public File performSparqlQuery(long entryId, String queryString) throws SQLException, IOException {

        File resultRdfFile = File.createTempFile("json-result-rdf-file", "json");

        GithubRepositoryOrderEntityLobs githubRepositoryOrderEntityLobs
                = entityManager.find(GithubRepositoryOrderEntityLobs.class, entryId);

        if (githubRepositoryOrderEntityLobs == null) {
            throw BadRequestException.githubEntryNotFound(entryId);
        }

        GithubRepositoryOrderEntity githubRepositoryOrderEntity = githubRepositoryOrderEntityLobs.getOrderEntity();

        if (!githubRepositoryOrderEntity.getStatus().equals(GitRepositoryOrderStatus.DONE)) {
            throw BadRequestException.githubToRdfConversionNotDone(entryId);
        }

        File tempRdfFile = null;

        try {

            tempRdfFile = getTempRdfFile(githubRepositoryOrderEntityLobs);

            Model rdfModel = ModelFactory.createDefaultModel();

            rdfModel.read(tempRdfFile.getAbsolutePath(), TURTLE_FORMAT);

            Query rdfQuery = QueryFactory.create(queryString);

            if (rdfQuery.isAskType()) {

                try (QueryExecution queryExecution = QueryExecutionFactory.create(rdfQuery, rdfModel)) {

                    boolean askResult = queryExecution.execAsk();

                    if (askResult) de.leipzig.htwk.gitrdf.sparql.query.utils.FileUtils.writeSimpleStringToFile("yes", resultRdfFile);
                    else de.leipzig.htwk.gitrdf.sparql.query.utils.FileUtils.writeSimpleStringToFile("no", resultRdfFile);

                }

            } else {

                try (QueryExecution queryExecution = QueryExecutionFactory.create(rdfQuery, rdfModel)) {

                    ResultSet resultSet = queryExecution.execSelect();

                    // source: https://www.w3.org/TR/sparql12-results-json/#json-result-object
                    try (OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(resultRdfFile))) {
                        ResultSetFormatter.outputAsJSON(outputStream, resultSet);
                    }

                }

            }

        } finally {
            if (tempRdfFile != null) FileUtils.deleteQuietly(tempRdfFile);
        }

        return resultRdfFile;
    }

    private File getTempRdfFile(GithubRepositoryOrderEntityLobs githubRepositoryOrderEntityLobs) throws IOException, SQLException {

        File tempRdfFile = File.createTempFile("temp-rdf-file", ".ttl");

        try (InputStream inputStream
                     = new BufferedInputStream(githubRepositoryOrderEntityLobs.getRdfFile().getBinaryStream())) {

            try (OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(tempRdfFile))) {

                byte[] buffer = new byte[BUFFER_SIZE];

                int length;

                while ((length = inputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, length);
                }

            }

        }

        return tempRdfFile;
    }


}
