@echo off

call mvn clean package -DskipTests

docker build -t git2rdflab/query-service:0.0.1-SNAPSHOT .
