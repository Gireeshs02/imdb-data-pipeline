CREATE DATABASE movies_db;
USE movies_db;

CREATE TABLE titles(
    tconst VARCHAR(255) PRIMARY KEY,
    titleType VARCHAR(50),
    primaryTitle VARCHAR(500),
    originalTitle VARCHAR(500),
    isAdult BOOLEAN,
    startYear INT,
    endYear INT,
    runtimeMinutes INT,
    genres VARCHAR(255)
);

CREATE TABLE ratings(
    tconst VARCHAR(255) PRIMARY KEY,
    averageRating DECIMAL(3, 1),
    numVotes INT
);

CREATE TABLE people(
    nconst VARCHAR(255) PRIMARY KEY,
    primaryName VARCHAR(255),
    birthYear INT,
    deathYear INT,
    primaryProfession VARCHAR(255),
    knownForTitles TEXT
);