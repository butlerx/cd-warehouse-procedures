SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = ERROR;
SET row_security = off;

SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS "dimBadges" (
    badge_id character varying(40) NOT NULL,
    type character varying(40),
    archived character varying(40),
    name character varying(100),
    id character varying(40),
    user_id character varying(40),
    issued_on timestamp,
    CONSTRAINT "dimBadges_pkey" PRIMARY KEY (badge_id)
);

CREATE TABLE IF NOT EXISTS "dimDojoLeads" (
    id character varying(40) NOT NULL,
    user_id character varying(40),
    confidence_mentoring smallint,
    confidence_coding smallint,
    venue_type character varying(40),
    alternative_venue_type character varying(5000),
    referer character varying(40),
    alternative_referer character varying(5000),
    has_mentors boolean,
    mentor_youth_workers boolean,
    mentor_it_professionals boolean,
    mentor_venue_staff boolean,
    mentor_parents boolean,
    mentor_teachers boolean,
    mentor_students boolean,
    mentor_youth_u18 boolean,
    mentor_other character varying(5000),
    requested_email boolean, 
    completion smallint,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    completed_at timestamp without time zone
);

CREATE TABLE IF NOT EXISTS "dimDojos" (
    id character varying(40) NOT NULL,
    created timestamp with time zone,
    stage smallint,
    country character varying(40),
    city character varying(100),
    state character varying(40),
    continent character varying(40),
    county character varying(40),
    tao_verified character varying(7),
    expected_attendees smallint,
    verified smallint,
    verified_at timestamp with time zone,
    deleted smallint,
    inactive smallint,
    inactive_at timestamp with time zone,
    is_eb smallint,
    lead_id character varying(40),
    url character varying(200),
    CONSTRAINT "PK_dimDojos" PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS "dimEvents" (
    id character varying(40) NOT NULL DEFAULT uuid_generate_v1(),
    event_id character varying(40) NOT NULL,
    recurring_type character varying(40),
    country character varying(40),
    city character varying(100),
    created_at timestamp without time zone,
    type character varying(40),
    dojo_id character varying(40),
    public boolean,
    is_eb boolean,
    status character varying(40),
    start_time timestamp without time zone,
    CONSTRAINT "dimEvents_pkey" PRIMARY KEY (id),
    CONSTRAINT "dimEvents_ukey" UNIQUE (event_id, start_time)
);

CREATE TABLE IF NOT EXISTS "dimLocation" (
    country character varying(40),
    city character varying(100),
    location_id character varying(40) NOT NULL,
    CONSTRAINT "dimLocation_pkey" PRIMARY KEY (location_id)
);

CREATE TABLE IF NOT EXISTS "dimTickets" (
    ticket_id character varying(40) NOT NULL,
    type character varying(40),
    quantity smallint,
    deleted smallint,
    CONSTRAINT "dimTickets_pkey" PRIMARY KEY (ticket_id)
);

CREATE TABLE IF NOT EXISTS "dimUsers" (
    dob timestamp without time zone,
    country character varying(50),
    city character varying(100),
    gender character varying(20),
    user_type character varying(40),
    roles character varying(40),
    user_id character varying(40) NOT NULL,
    mailing_list smallint DEFAULT 0 NOT NULL,
    created_at timestamp without time zone,
    CONSTRAINT "dimUsers_pkey" PRIMARY KEY (user_id)
);

CREATE TABLE IF NOT EXISTS "factUsers" (
    dojo_id character varying(40),
    badge_id character varying(40),
    event_id character varying(40),
    ticket_id character varying(40),
    user_id character varying(40),
    time timestamp without time zone,
    location_id character varying(40),
    id character varying(40) NOT NULL,
    checked_in boolean,
    CONSTRAINT "factUsers_pkey" PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS staging (
    user_id character varying(40),
    dojo_id character varying(40),
    event_id character varying(40),
    session_id character varying(40),
    ticket_id character varying(40),
    id character varying(40) NOT NULL,
    time timestamp without time zone,
    location_id character varying(40),
    badge_id character varying(40),
    checked_in boolean,
    CONSTRAINT "PK_staging" PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS "dimUsersDojos" (
  id character varying NOT NULL,
  user_id character varying,
  dojo_id character varying,
  user_type character varying
);

CREATE INDEX "FKI_factUsers_dimBadges" ON "factUsers" USING btree (badge_id);

CREATE INDEX "FKI_factUsers_dimDojos" ON "factUsers" USING btree (dojo_id);

CREATE INDEX "FKI_factUsers_dimEvents" ON "factUsers" USING btree (event_id);

CREATE INDEX "FKI_factUsers_dimLocation" ON "factUsers" USING btree (location_id);

CREATE INDEX "FKI_factUsers_dimTickets" ON "factUsers" USING btree (ticket_id);

ALTER TABLE ONLY "factUsers"
    ADD CONSTRAINT "FK_factUsers_dimBadges" FOREIGN KEY (badge_id) REFERENCES "dimBadges"(badge_id);

ALTER TABLE ONLY "factUsers"
    ADD CONSTRAINT "FK_factUsers_dimDojos" FOREIGN KEY (dojo_id) REFERENCES "dimDojos"(id);

ALTER TABLE ONLY "factUsers"
    ADD CONSTRAINT "FK_factUsers_dimLocation" FOREIGN KEY (location_id) REFERENCES "dimLocation"(location_id);

ALTER TABLE ONLY "factUsers"
    ADD CONSTRAINT "FK_factUsers_dimTickets" FOREIGN KEY (ticket_id) REFERENCES "dimTickets"(ticket_id);

ALTER TABLE ONLY "factUsers"
    ADD CONSTRAINT "FK_factUsers_dimUsers" FOREIGN KEY (user_id) REFERENCES "dimUsers"(user_id);
