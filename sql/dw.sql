--
-- PostgresQL database dump
--

-- Dumped from database version 9.6.1
-- Dumped by pg_dump version 9.6.1

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: zen-source; Type: SCHEMA; Schema: -; Owner: platform
--

CREATE SCHEMA "zen-source";
ALTER SCHEMA "zen-source" OWNER TO platform;

SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: dimBadges; Type: TABLE; Schema: public; Owner: platform
--

CREATE TABLE "dimBadges" (
    badge_id character varying(40) NOT NULL,
    type character varying(40),
    archived character varying(40),
    name character varying(100),
    id character varying(40),
    user_id character varying(40)
);


ALTER TABLE "dimBadges" OWNER TO platform;

--
-- Name: dimDojos; Type: TABLE; Schema: public; Owner: platform
--

CREATE TABLE "dimDojos" (
    id character varying(40) NOT NULL,
    created timestamp without time zone,
    stage smallint,
    country character varying(40),
    city character varying(100),
    state character varying(40),
    continent character varying(40),
    tao_verified character varying(7),
    expected_attendees smallint,
    verified smallint,
    deleted smallint,
    verified_at timestamp with time zone,
    county character varying(40)
);


ALTER TABLE "dimDojos" OWNER TO platform;

--
-- Name: dimEvents; Type: TABLE; Schema: public; Owner: platform
--

CREATE TABLE "dimEvents" (
    event_id character varying(40) NOT NULL,
    recurring_type character varying(40),
    country character varying(40),
    city character varying(100),
    created_at timestamp without time zone,
    type character varying(40),
    dojo_id character varying(40),
    public boolean,
    status character varying(40)
);


ALTER TABLE "dimEvents" OWNER TO platform;

--
-- Name: dimLocation; Type: TABLE; Schema: public; Owner: platform
--

CREATE TABLE "dimLocation" (
    country character varying(40),
    city character varying(100),
    location_id character varying(40) NOT NULL
);


ALTER TABLE "dimLocation" OWNER TO platform;

--
-- Name: dimTickets; Type: TABLE; Schema: public; Owner: platform
--

CREATE TABLE "dimTickets" (
    ticket_id character varying(40) NOT NULL,
    type character varying(40),
    quantity smallint,
    deleted smallint
);


ALTER TABLE "dimTickets" OWNER TO platform;

--
-- Name: dimTime; Type: TABLE; Schema: public; Owner: platform
--

CREATE TABLE "dimTime" (
    year smallint,
    month smallint,
    day smallint,
    time_id character varying(40) NOT NULL
);


ALTER TABLE "dimTime" OWNER TO platform;

--
-- Name: dimUsers; Type: TABLE; Schema: public; Owner: platform
--

CREATE TABLE "dimUsers" (
    dob timestamp without time zone,
    country character varying(50),
    city character varying(100),
    gender character varying(11),
    user_type character varying(40),
    roles character varying(40),
    continent character varying(40),
    user_id character varying(40) NOT NULL,
    mailing_list smallint DEFAULT 0 NOT NULL,
    age integer
);


ALTER TABLE "dimUsers" OWNER TO platform;

--
-- Name: factUsers; Type: TABLE; Schema: public; Owner: platform
--

CREATE TABLE "factUsers" (
    dojo_id character varying(40),
    badge_id character varying(40),
    event_id character varying(40),
    ticket_id character varying(40),
    user_id character varying(40),
    time_id character varying(40),
    location_id character varying(40),
    id character varying(40) NOT NULL
);


ALTER TABLE "factUsers" OWNER TO platform;

SET search_path = "zen-source", pg_catalog;

--
-- Name: staging; Type: TABLE; Schema: zen-source; Owner: platform
--

CREATE TABLE staging (
    user_id character varying(40),
    dojo_id character varying(40),
    event_id character varying(40),
    session_id character varying(40),
    ticket_id character varying(40),
    id character varying(40) NOT NULL,
    time_id character varying(40),
    location_id character varying(40),
    badge_id character varying(40)
);


ALTER TABLE staging OWNER TO platform;

SET search_path = public, pg_catalog;

--
-- Data for Name: dimBadges; Type: TABLE DATA; Schema: public; Owner: platform
--

-- COPY "dimBadges" (badge_id, type, archived, name, id, user_id) FROM stdin;
-- \.

--
-- Data for Name: dimDojos; Type: TABLE DATA; Schema: public; Owner: platform
--

-- COPY "dimDojos" (id, created, stage, country, city, state, continent, tao_verified, expected_attendees, verified, deleted, verified_at, county) FROM stdin;
-- \.


--
-- Data for Name: dimEvents; Type: TABLE DATA; Schema: public; Owner: platform
--

-- COPY "dimEvents" (event_id, recurring_type, country, city, created_at, type, dojo_id, public, status) FROM stdin;
-- \.


--
-- Data for Name: dimLocation; Type: TABLE DATA; Schema: public; Owner: platform
--

-- COPY "dimLocation" (country, city, location_id) FROM stdin;
-- \.


--
-- Data for Name: dimTickets; Type: TABLE DATA; Schema: public; Owner: platform
--

-- COPY "dimTickets" (ticket_id, type, quantity, deleted) FROM stdin;
-- \.


--
-- Data for Name: dimTime; Type: TABLE DATA; Schema: public; Owner: platform
--

-- COPY "dimTime" (year, month, day, time_id) FROM stdin;
-- \.


--
-- Data for Name: dimUsers; Type: TABLE DATA; Schema: public; Owner: platform
--

-- COPY "dimUsers" (dob, country, city, gender, user_type, roles, continent, user_id, mailing_list, age) FROM stdin;
-- \.


--
-- Data for Name: factUsers; Type: TABLE DATA; Schema: public; Owner: platform
--

-- COPY "factUsers" (dojo_id, badge_id, event_id, ticket_id, user_id, time_id, location_id, id) FROM stdin;
-- \.


SET search_path = "zen-source", pg_catalog;

--
-- Data for Name: staging; Type: TABLE DATA; Schema: zen-source; Owner: platform
--

-- COPY staging (user_id, dojo_id, event_id, session_id, ticket_id, id, time_id, location_id, badge_id) FROM stdin;
-- \.


SET search_path = public, pg_catalog;

--
-- Name: dimDojos PK_dimDojos; Type: CONSTRAINT; Schema: public; Owner: platform
--

ALTER TABLE ONLY "dimDojos"
    ADD CONSTRAINT "PK_dimDojos" PRIMARY KEY (id);


--
-- Name: dimBadges dimBadges_pkey; Type: CONSTRAINT; Schema: public; Owner: platform
--

ALTER TABLE ONLY "dimBadges"
    ADD CONSTRAINT "dimBadges_pkey" PRIMARY KEY (badge_id);


--
-- Name: dimEvents dimEvents_pkey; Type: CONSTRAINT; Schema: public; Owner: platform
--

ALTER TABLE ONLY "dimEvents"
    ADD CONSTRAINT "dimEvents_pkey" PRIMARY KEY (event_id);


--
-- Name: dimLocation dimLocation_pkey; Type: CONSTRAINT; Schema: public; Owner: platform
--

ALTER TABLE ONLY "dimLocation"
    ADD CONSTRAINT "dimLocation_pkey" PRIMARY KEY (location_id);


--
-- Name: dimTickets dimTickets_pkey; Type: CONSTRAINT; Schema: public; Owner: platform
--

ALTER TABLE ONLY "dimTickets"
    ADD CONSTRAINT "dimTickets_pkey" PRIMARY KEY (ticket_id);


--
-- Name: dimTime dimTime_pkey; Type: CONSTRAINT; Schema: public; Owner: platform
--

ALTER TABLE ONLY "dimTime"
    ADD CONSTRAINT "dimTime_pkey" PRIMARY KEY (time_id);


--
-- Name: dimUsers dimUsers_pkey; Type: CONSTRAINT; Schema: public; Owner: platform
--

ALTER TABLE ONLY "dimUsers"
    ADD CONSTRAINT "dimUsers_pkey" PRIMARY KEY (user_id);


--
-- Name: factUsers factUsers_pkey; Type: CONSTRAINT; Schema: public; Owner: platform
--

ALTER TABLE ONLY "factUsers"
    ADD CONSTRAINT "factUsers_pkey" PRIMARY KEY (id);


SET search_path = "zen-source", pg_catalog;

--
-- Name: staging PK_staging; Type: CONSTRAINT; Schema: zen-source; Owner: platform
--

ALTER TABLE ONLY staging
    ADD CONSTRAINT "PK_staging" PRIMARY KEY (id);


SET search_path = public, pg_catalog;

--
-- Name: FKI_factUsers_dimBadges; Type: INDEX; Schema: public; Owner: platform
--

CREATE INDEX "FKI_factUsers_dimBadges" ON "factUsers" USING btree (badge_id);


--
-- Name: FKI_factUsers_dimDojos; Type: INDEX; Schema: public; Owner: platform
--

CREATE INDEX "FKI_factUsers_dimDojos" ON "factUsers" USING btree (dojo_id);


--
-- Name: FKI_factUsers_dimEvents; Type: INDEX; Schema: public; Owner: platform
--

CREATE INDEX "FKI_factUsers_dimEvents" ON "factUsers" USING btree (event_id);


--
-- Name: FKI_factUsers_dimLocation; Type: INDEX; Schema: public; Owner: platform
--

CREATE INDEX "FKI_factUsers_dimLocation" ON "factUsers" USING btree (location_id);


--
-- Name: FKI_factUsers_dimTickets; Type: INDEX; Schema: public; Owner: platform
--

CREATE INDEX "FKI_factUsers_dimTickets" ON "factUsers" USING btree (ticket_id);


--
-- Name: FKI_factUsers_dimTime; Type: INDEX; Schema: public; Owner: platform
--

CREATE INDEX "FKI_factUsers_dimTime" ON "factUsers" USING btree (time_id);


--
-- Name: FKI_factUsers_dimUsers; Type: INDEX; Schema: public; Owner: platform
--

CREATE INDEX "FKI_factUsers_dimUsers" ON "factUsers" USING btree (user_id);


--
-- Name: factUsers FK_factUsers_dimBadges; Type: FK CONSTRAINT; Schema: public; Owner: platform
--

ALTER TABLE ONLY "factUsers"
    ADD CONSTRAINT "FK_factUsers_dimBadges" FOREIGN KEY (badge_id) REFERENCES "dimBadges"(badge_id);


--
-- Name: factUsers FK_factUsers_dimDojos; Type: FK CONSTRAINT; Schema: public; Owner: platform
--

ALTER TABLE ONLY "factUsers"
    ADD CONSTRAINT "FK_factUsers_dimDojos" FOREIGN KEY (dojo_id) REFERENCES "dimDojos"(id);


--
-- Name: factUsers FK_factUsers_dimEvents; Type: FK CONSTRAINT; Schema: public; Owner: platform
--

ALTER TABLE ONLY "factUsers"
    ADD CONSTRAINT "FK_factUsers_dimEvents" FOREIGN KEY (event_id) REFERENCES "dimEvents"(event_id);


--
-- Name: factUsers FK_factUsers_dimLocation; Type: FK CONSTRAINT; Schema: public; Owner: platform
--

ALTER TABLE ONLY "factUsers"
    ADD CONSTRAINT "FK_factUsers_dimLocation" FOREIGN KEY (location_id) REFERENCES "dimLocation"(location_id);


--
-- Name: factUsers FK_factUsers_dimTickets; Type: FK CONSTRAINT; Schema: public; Owner: platform
--

ALTER TABLE ONLY "factUsers"
    ADD CONSTRAINT "FK_factUsers_dimTickets" FOREIGN KEY (ticket_id) REFERENCES "dimTickets"(ticket_id);


--
-- Name: factUsers FK_factUsers_dimTime; Type: FK CONSTRAINT; Schema: public; Owner: platform
--

ALTER TABLE ONLY "factUsers"
    ADD CONSTRAINT "FK_factUsers_dimTime" FOREIGN KEY (time_id) REFERENCES "dimTime"(time_id);


--
-- Name: factUsers FK_factUsers_dimUsers; Type: FK CONSTRAINT; Schema: public; Owner: platform
--

ALTER TABLE ONLY "factUsers"
    ADD CONSTRAINT "FK_factUsers_dimUsers" FOREIGN KEY (user_id) REFERENCES "dimUsers"(user_id);


--
-- PostgreSQL database dump complete
--

