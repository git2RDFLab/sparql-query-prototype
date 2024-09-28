# Prototype for performing SPARQL-Queries onto finished rdf conversion entries

## Dependency notice

In order to remove duplicate database jpa definitions, a shared database commons project was introduced, which is used
in all projects, that share the mentioned jpa entities and logic.
The project can be found under: https://github.com/git2RDFLab/database-shared-common

In order to compile this project you have to pull the shared database common project and install the maven artifact locally
(via: `mvn clean install`) as dependency, so this project can find the necessary dependency.

The database shared common dependency is already included in this project as a dependency in the pom with

```
<dependency>
	<groupId>de.leipzig.htwk.gitrdf.database</groupId>
	<artifactId>common</artifactId>
	<version>${de.leipzig.htwk.gitrdf.database.common.version}</version>
</dependency>
```

## Environment Variables

| Environment Variables      | Description                                                                                                                                                                                                                           |
|----------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| SPRING_DATASOURCE_URL      | The fully qualified url to the database. Expects the database connection string as of the defined schema by the used database. This projects expects per default a postgres database. A default value is given for local deployments. |
| SPRING_DATASOURCE_PASSWORD | The password of the database. A default value is given for local deployments.                                                                                                                                                         |

## Spring Initializr Template
https://start.spring.io/#!type=maven-project&language=java&platformVersion=3.2.3&packaging=jar&jvmVersion=21&groupId=de.leipzig.htwk.gitrdf.sparql&artifactId=query&name=query&description=Archetype%20project%20for%20HTWK%20Leipzig%20-%20Project%20to%20transform%20git%20to%20RDF&packageName=de.leipzig.htwk.gitrdf.sparql.query&dependencies=web,lombok,devtools,data-jpa,postgresql,testcontainers


## CURL-Example to perform SPARQL-Query and to get result
curl -XPOST -H "Content-type: application/sparql-query" -d $'PREFIX git: <git://>\n\nSELECT ?commit WHERE { ?commit git:AuthorName "emmanuel" . }' localhost:7080/query-service/api/v1/github/rdf/query/{id} -o "query-result.json"

## SPARQL-Query execution alternatives
SPARQL-Queries can also be performed by going to https://yasgui.triply.cc/ and using its api
