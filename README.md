<a href="https://github.com/git2RDFLab/"><img align="right" role="right" height="96" src="https://github.com/git2RDFLab/.github/blob/main/profile/images/GitLotus-logo.png?raw=true" style="height: 96px;z-index: 1000000" title="GitLotus" alt="GitLotus"/></a>

# GitLotus component -- Web service handling SPARQL queries on RDF-converted Git repositories

The component is available as a Docker image [superdose/git2rdf-query-service](https://hub.docker.com/r/superdose/git2rdf-query-service/tags).
See the repository [project-deployment-compose](https://github.com/git2RDFLab/project-deployment-compose/tree/main) for a prepared Docker container starting and configuration script.

## Build and execution environment

The [Spring Boot](https://spring.io/projects/spring-boot) service can be created using [Apache Maven](https://maven.apache.org/).

```ShellSession
git clone git@github.com:git2RDFLab/sparql-query-prototype.git
cd sparql-query-prototype
mvn package
```

See the folder `target` for the executable JAR file.

**Dependency notice:** To remove duplicate database [JPA](https://spring.io/projects/spring-data-jpa) definitions, a shared database commons project was introduced. See [database-shared-common](https://github.com/git2RDFLab/database-shared-common/) for installing this GitLotus-specific dependency.

## Environment Variables

| Environment Variables      | Description                                                                                                                                                                                                                           |
|----------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `SPRING_DATASOURCE_URL`      | The fully qualified URL to the database. Expects the database connection string as of the defined schema by the used database. This project expects per default a PostgreSQL database. A default value is given for local deployments. |
| `SPRING_DATASOURCE_PASSWORD` | The password of the database. A default value is given for local deployments.                                                                                                                                                         |

[Spring Initializr Template](https://start.spring.io/#!type=maven-project&language=java&platformVersion=3.2.3&packaging=jar&jvmVersion=21&groupId=de.leipzig.htwk.gitrdf.sparql&artifactId=query&name=query&description=Archetype%20project%20for%20HTWK%20Leipzig%20-%20Project%20to%20transform%20git%20to%20RDF&packageName=de.leipzig.htwk.gitrdf.sparql.query&dependencies=web,lombok,devtools,data-jpa,postgresql,testcontainers)


## Example requests

### CURL example to perform a SPARQL query and to get JSON result

```ShellSession
curl -XPOST -H "Content-type: application/sparql-query" -d $'PREFIX git: <git://>\n\nSELECT ?commit WHERE { ?commit git:AuthorName "emmanuel" . }' localhost:7080/query-service/api/v1/github/rdf/query/{id} -o "query-result.json"
```

### SPARQL query execution alternatives

SPARQL queries can also be performed by using https://yasgui.triply.cc/.

## Contribute

We are happy to receive your contributions. 
Please create a pull request or an issue. 
As this tool is published under the MIT license, feel free to fork it and use it in your own projects.

## Disclaimer

This tool just temporarily stores the image data. 
It is provided "as is" and without any warranty, express or implied.
