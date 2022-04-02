DROP SCHEMA IF EXISTS shorttrack CASCADE;


create schema shorttrack;
drop table if exists shorttrack.athlete CASCADE;
drop table if exists shorttrack.countries CASCADE;
drop table if exists shorttrack.relay_sfnl CASCADE;
drop table if exists shorttrack.relay_fnl CASCADE;
drop table if exists shorttrack.qfnl CASCADE;
drop table if exists shorttrack.sfnl CASCADE;
drop table if exists shorttrack.fnl CASCADE;

CREATE TABLE shorttrack.countries (
  "country_code" varchar(256) PRIMARY KEY,
  "name" varchar(256)
);

CREATE TABLE shorttrack.athlete (
  "id" Varchar(256) PRIMARY KEY,
  "name" varchar(256),
  "country_code" varchar(256),
  "age" int,
  "gender" varchar(256)
);



CREATE TABLE shorttrack.relay_sfnl (
  "country_code" varchar(256) references shorttrack.countries("country_code"),
  "game_type" varchar(256),
  "time" time,
  "rank" int,
  "qualified" varchar(256)
);

CREATE TABLE shorttrack.relay_fnl (
  "country_code" varchar(256) references shorttrack.countries("country_code"),
  "game_type" varchar(256),
  "time" time,
  "rank" int
);

CREATE TABLE shorttrack.qfnl (
  "id" varchar(256) references shorttrack.athlete("id"),
  "game_type" varchar(256),
  "time" time,
  "rank" int,
  "qualified" varchar(256)
);

CREATE TABLE shorttrack.sfnl (
  "id" varchar(256) references shorttrack.athlete("id"),
  "game_type" varchar(256),
  "time" time,
  "rank" int,
  "qualified" varchar(256)
);

CREATE TABLE shorttrack.fnl (
  "id" varchar(256)  references shorttrack.athlete("id"),
  "game_type" varchar(256),
  "time" time,
  "rank" int
);
