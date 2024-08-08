package de.leipzig.htwk.gitrdf.sparql.query.service.impl;

import de.leipzig.htwk.gitrdf.database.common.entity.GithubRepositoryOrderEntity;
import de.leipzig.htwk.gitrdf.database.common.entity.enums.GitRepositoryOrderStatus;
import de.leipzig.htwk.gitrdf.database.common.entity.lob.GithubRepositoryOrderEntityLobs;
import de.leipzig.htwk.gitrdf.sparql.query.api.exception.BadRequestException;
import de.leipzig.htwk.gitrdf.sparql.query.api.exception.NotFoundException;
import de.leipzig.htwk.gitrdf.sparql.query.timemeasurement.TimeLog;
import jakarta.persistence.EntityManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.*;
import java.sql.SQLException;

@Service
@RequiredArgsConstructor
@Slf4j
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
            throw NotFoundException.githubEntryNotFound(entryId);
        }

        GithubRepositoryOrderEntity githubRepositoryOrderEntity = githubRepositoryOrderEntityLobs.getOrderEntity();

        if (!githubRepositoryOrderEntity.getStatus().equals(GitRepositoryOrderStatus.DONE)) {
            throw BadRequestException.githubToRdfConversionNotDone(entryId);
        }

        File tempRdfFile = null;

        try {
            log.info("Start Query");
            log.info("Warming up JVM");
            performWarmUpQuery(githubRepositoryOrderEntityLobs);
            TimeLog timeLog = new TimeLog();
            timeLog.setEntryId(entryId);

            long totalElapsedTime = 0;

            for (int i = 0; i < 10; i++) {
                StopWatch watch = new StopWatch();
                watch.start();

                try {
                    tempRdfFile = getTempRdfFile(githubRepositoryOrderEntityLobs);
                    Model rdfModel = ModelFactory.createDefaultModel();
                    rdfModel.read(tempRdfFile.getAbsolutePath(), TURTLE_FORMAT);

                    Query rdfQuery = QueryFactory.create(queryString);

                    if (rdfQuery.isAskType()) {
                        try (QueryExecution queryExecution = QueryExecutionFactory.create(rdfQuery, rdfModel)) {
                            boolean askResult = queryExecution.execAsk();
                            if (askResult) {
                                de.leipzig.htwk.gitrdf.sparql.query.utils.FileUtils.writeSimpleStringToFile("yes", resultRdfFile);
                            } else {
                                de.leipzig.htwk.gitrdf.sparql.query.utils.FileUtils.writeSimpleStringToFile("no", resultRdfFile);
                            }
                        }
                    } else {
                        try (QueryExecution queryExecution = QueryExecutionFactory.create(rdfQuery, rdfModel)) {
                            ResultSet resultSet = queryExecution.execSelect();
                            try (OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(resultRdfFile))) {
                                ResultSetFormatter.outputAsJSON(outputStream, resultSet);
                            }
                        }
                    }
                } finally {
                    if (tempRdfFile != null) {
                        FileUtils.deleteQuietly(tempRdfFile);
                    }
                }

                watch.stop();
                long elapsedTime = watch.getTime();
                totalElapsedTime += elapsedTime;

                log.info("Iteration " + (i + 1) + " completed in " + elapsedTime + " ms");
            }

            long averageTime = totalElapsedTime / 10;
            timeLog.setTotalTime(averageTime);
            timeLog.printTimes();


        } finally {
            if (tempRdfFile != null) FileUtils.deleteQuietly(tempRdfFile);
        }

        return resultRdfFile;
    }

    private void performWarmUpQuery(GithubRepositoryOrderEntityLobs githubRepositoryOrderEntityLobs) {
        File tempRdfFile = null;
        try {
            // Temporäre RDF-Datei erstellen
            tempRdfFile = getTempRdfFile(githubRepositoryOrderEntityLobs);

            // RDF-Modell erstellen und Datei einlesen
            Model rdfModel = ModelFactory.createDefaultModel();
            rdfModel.read(tempRdfFile.getAbsolutePath(), TURTLE_FORMAT);

            // Eine einfache SPARQL-Abfrage erstellen
            String warmUpQueryString = "ASK WHERE { ?s ?p ?o }";
            Query warmUpQuery = QueryFactory.create(warmUpQueryString);

            // Abfrage ausführen (in diesem Fall eine einfache ASK-Abfrage)
            try (QueryExecution queryExecution = QueryExecutionFactory.create(warmUpQuery, rdfModel)) {
                queryExecution.execAsk(); // Ergebnis ist irrelevant, es geht nur um das Ausführen der Abfrage
            }

        } catch (Exception e) {
            log.error("Error during JVM warm-up query", e);
        } finally {
            // Temporäre Datei löschen
            if (tempRdfFile != null) {
                FileUtils.deleteQuietly(tempRdfFile);
            }
        }
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
