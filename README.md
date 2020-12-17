# Seminararbeit II DWH - Partitioning in PostgreSQL
(C) Julian Stein

## Docker implementation
An image containing the preprocessed JSON-Data and a full script is available at [dockerhub](https://hub.docker.com/repository/docker/steinju/seminararbeit_dwh).
Run the container using
```
docker run -p 5432:5432 -e POSTGRES_PASSWORD=setpassword --name seminararbeit_dwh_container steinju/seminararbeit_dwh:latest
```
On start-up the full SQL script is executed. This may take a while (about 30 minutes in my test run).
