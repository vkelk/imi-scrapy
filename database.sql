-- DROP SCHEMA imi;

CREATE SCHEMA imi AUTHORIZATION postgres;

-- imi.calls definition

-- Drop table

-- DROP TABLE imi.calls;

CREATE TABLE imi.calls (
	id serial NOT NULL,
	call_id varchar NULL,
	call_date date NULL,
	action_type varchar NULL,
	proposal_submitted int4 NULL,
	topics varchar NULL,
	indicative_budget varchar NULL,
	publication_date date NULL,
	sub_start_date date NULL,
	sub_end_date date NULL,
	url varchar NULL,
	CONSTRAINT calls_pk PRIMARY KEY (id)
);
CREATE INDEX calls_call_id_idx ON imi.calls USING btree (call_id);

-- imi.projects definition

-- Drop table

-- DROP TABLE imi.projects;

CREATE TABLE imi.projects (
	gan int4 NOT NULL,
	project_name varchar NULL,
	start_date date NULL,
	end_date date NULL,
	call_id varchar NULL,
	status varchar NULL,
	"program" varchar NULL,
	disease_area varchar NULL,
	products varchar NULL,
	tools varchar NULL,
	imi_funding int4 NULL,
	efpia_inkind int4 NULL,
	other int4 NULL,
	project_intro varchar NULL,
	project_website varchar NULL,
	twitter_handle varchar NULL,
	project_coordinator varchar NULL,
	project_leader varchar NULL,
	project_manager varchar NULL,
	url varchar NULL,
	summary varchar NULL,
	CONSTRAINT projects_pk PRIMARY KEY (gan)
);

-- imi.fundings definition

-- Drop table

-- DROP TABLE imi.fundings;

CREATE TABLE imi.fundings (
	id serial NOT NULL,
	"name" varchar NULL,
	funding int4 NULL,
	gan int4 NULL,
	raw_text varchar NULL,
	CONSTRAINT fundings_pk PRIMARY KEY (id)
);


-- imi.fundings foreign keys

ALTER TABLE imi.fundings ADD CONSTRAINT fundings_fk FOREIGN KEY (gan) REFERENCES imi.projects(gan) ON UPDATE CASCADE ON DELETE CASCADE;

-- imi.participants definition

-- Drop table

-- DROP TABLE imi.participants;

CREATE TABLE imi.participants (
	id serial NOT NULL,
	gan int4 NULL,
	"name" varchar NULL,
	"type" varchar NULL,
	city varchar NULL,
	region varchar NULL,
	country varchar NULL,
	raw_text varchar NULL,
	CONSTRAINT participants_pk PRIMARY KEY (id)
);


-- imi.participants foreign keys

ALTER TABLE imi.participants ADD CONSTRAINT participants_fk FOREIGN KEY (gan) REFERENCES imi.projects(gan) ON UPDATE CASCADE ON DELETE CASCADE;

ALTER TABLE imi.projects ADD leader_company varchar NULL;
