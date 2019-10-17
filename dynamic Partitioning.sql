
-- DROP PROCEDURE IF EXISTS wh.tidy_tabel;
CREATE OR REPLACE PROCEDURE wh.tidy_tabel(org_tabel CHARACTER VARYING = '', org_schema CHARACTER VARYING = '') AS
$FBODY$
DECLARE
_resultaten RECORD;
_tmptabel CHARACTER VARYING;
_full_tabel CHARACTER VARYING;
_default_tabel CHARACTER VARYING;
_partitioning_column CHARACTER VARYING;

BEGIN
		IF (org_tabel IS NOT NULL AND org_tabel != '') THEN
			IF (org_schema = '') THEN org_schema = 'wh'; END IF;
			_tmptabel := CONCAT('_tmp_',org_tabel,'_',TO_CHAR(TRANSACTION_TIMESTAMP(),'YYYYMMDDHH24MISSMSUS'));
			_full_tabel := CONCAT(org_schema,'.',org_tabel);
			_default_tabel := CONCAT(org_schema,'.',org_tabel,'_pdefault');

			-- Uitzoeken op welke kolom gepartitioned wordt
			SELECT SPLIT_PART(SPLIT_PART(pg_get_partkeydef(c.oid) , '(',2 ),')',1) INTO _partitioning_column
			FROM	pg_catalog.pg_class AS c,
					pg_catalog.pg_namespace AS n
			WHERE 	n.oid = c.relnamespace AND
					c.relkind = 'p' AND
					c.relname = org_tabel AND
					n.nspname = org_schema
			;

			EXECUTE 'LOCK TABLE ' || _full_tabel || ' IN EXCLUSIVE MODE';
			EXECUTE 'CREATE TEMPORARY TABLE ' || _tmptabel || '(LIKE ' || _full_tabel || ');';
			EXECUTE 'INSERT INTO ' || _tmptabel || ' SELECT * FROM ONLY ' || _default_tabel || ';';
			EXECUTE 'TRUNCATE TABLE ONLY ' || _default_tabel || ';';

			FOR _resultaten IN
					EXECUTE ('	SELECT in_def.ptabel,in_def.startdate, in_def.enddate
								FROM (	SELECT DISTINCT CONCAT (''' || org_tabel || ''',''_p'', TO_CHAR ( ' || _partitioning_column || ',''YYYYMM''))::NAME AS ptabel,
										TO_CHAR(DATE_TRUNC(''MONTH'',' || _partitioning_column || '),''YYYY-MM-DD'') AS startdate,
										TO_CHAR(DATE_TRUNC(''MONTH'',' || _partitioning_column || ' + INTERVAL ''1 MONTH''),''YYYY-MM-DD'') AS enddate
										FROM ' || _tmptabel || ') AS in_def
								LEFT JOIN
									(	SELECT relname AS ptabel
										FROM   pg_catalog.pg_class c
										JOIN   pg_catalog.pg_namespace n
										ON n.oid = c.relnamespace
										WHERE  	c.relkind = ''r'' AND
												c.relname ~ CONCAT(''^'',''' || org_tabel || ''') AND
												n.nspname =''' || org_schema || ''') AS found
								ON	in_def.ptabel = found.ptabel
								WHERE found.ptabel IS NULL')
			LOOP
				-- Maak de nieuwe partities aan:
				EXECUTE 'CREATE TABLE ' || org_schema || '.' || _resultaten.ptabel || ' PARTITION OF ' || org_schema || '.' || org_tabel || ' FOR VALUES FROM (' || quote_literal(_resultaten.startdate) || ') TO (' || quote_literal(_resultaten.enddate) || ');';
			END LOOP;

			EXECUTE 'INSERT INTO ' || _full_tabel || '  SELECT * FROM ' || _tmptabel || ';';
			RAISE NOTICE 'Tabel % opgeschoond.', _full_tabel;
		ELSE
			RAISE NOTICE 'Geen tabel opgegeven om op te schonen - geen actie ondernomen.';
		END IF;
END;
$FBODY$
LANGUAGE plpgsql
-- PARALLEL UNSAFE
SECURITY DEFINER
;

-- Table: verstrekking
-- DROP TABLE verstrekking;
CREATE TABLE wh.verstrekking (
	verstrekking_id SERIAL NOT NULL,
	apotheek_id INTEGER NOT NULL,
	a_naam CHARACTER VARYING(63) NOT NULL,
	registratiedatum TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
	verstrekking_datum TIMESTAMP(6) WITH TIME ZONE NOT NULL DEFAULT STATEMENT_TIMESTAMP(),
	patient_id INTEGER NULL,
	apotheek_patient_nummer INTEGER NOT NULL,
	artikel_id INTEGER NULL,
	zindex_nummer_apotheek INTEGER NULL,
	artikelcode_apotheek CHARACTER VARYING(5) NULL,
	verstrekking_recept_code CHARACTER VARYING(12) NULL,
	gebruik_per_dag NUMERIC(8,2) NULL,
	verwachtte_einddatum_medicatie DATE NOT NULL,
	herhalingen CHARACTER VARYING(10) NULL,
	verstrekte_hoeveelheid_medicatie NUMERIC(8,2) NULL,
	instelling_code CHARACTER VARYING(2) NULL, /* WEG? */
	bewakingssignaal_id INTEGER NULL,
	specialisme_id INTEGER NULL,
	is_anz BOOLEAN NULL DEFAULT FALSE,
	is_eerste_uitgifte BOOLEAN NULL,
	is_vervolg_uitgifte BOOLEAN NULL,
	is_zelfzorgmaatregel_continu_gebruik BOOLEAN NULL, /* WEG? */
	is_zelfzorgmaatregel_eerste_verstrekking BOOLEAN NULL, /* WEG? */
	is_bijzondere_bereiding BOOLEAN NULL,
	is_met_begeleidend_gesprek BOOLEAN NULL,
	is_met_thuistoeslag BOOLEAN NOT NULL DEFAULT FALSE,
	week_levering INTEGER NULL,
	verkoopprijs NUMERIC(8,2) NULL,
	verkoopprijs_netto NUMERIC(8,2) NULL, /* WEG? */
	inkoopprijs NUMERIC(8,2) NULL, /* WEG? */
	bijbetaling_bedrag NUMERIC(8,2) NULL, /* WEG? */
	clawback NUMERIC(8,2) NULL, /* WEG? */
	etiket_tekst CHARACTER VARYING(40) NULL, /* WEG? */
	etiket_tekst_toevoeging CHARACTER VARYING(200) NULL, /* WEG? */
	etiket_gebruiksadvies CHARACTER VARYING(200) NULL, /* WEG? */
	etiket_waarschuwing CHARACTER VARYING(200) NULL, /* WEG? */
	-- CONSTRAINT pk_verstrekking PRIMARY KEY (verstrekking_id),
	CONSTRAINT fk_verstrekking__apotheek FOREIGN KEY (apotheek_id) REFERENCES wh.apotheek (apotheek_id) ON DELETE CASCADE ON UPDATE NO ACTION,
	CONSTRAINT fk_verstrekking__patient FOREIGN KEY (patient_id) REFERENCES patient (patient_id) ON UPDATE NO ACTION ON DELETE CASCADE
-- ) PARTITION BY RANGE (DATE_TRUNC('MONTH', verstrekking_datum AT TIME ZONE 'UTC'));
) PARTITION BY RANGE (verstrekking_datum);


ALTER TABLE wh.verstrekking OWNER TO dwh_owner;
GRANT SELECT ON TABLE wh.verstrekking TO dwh_readonly;

CREATE TABLE wh.verstrekking_pdefault PARTITION OF wh.verstrekking DEFAULT;
ALTER TABLE wh.verstrekking_pdefault OWNER TO dwh_owner;

-- Index: idx_verstrekking__apotheek_id
-- DROP INDEX idx_verstrekking__apotheek_id;
CREATE INDEX idx_verstrekking__apotheek_id ON wh.verstrekking (apotheek_id, verstrekking_datum, artikel_id, patient_id);
-- Index: idx_verstrekking__uitgifte_datum
-- DROP INDEX idx_verstrekking__uitgifte_datum;
CREATE INDEX idx_verstrekking__uitgifte_datum ON verstrekking USING btree(verstrekking_datum, verstrekking_id, apotheek_id, patient_id, artikel_id, apotheek_patient_nummer, verwachtte_einddatum_medicatie);

-- Index: idx_verstrekking__registratiedatum
-- DROP INDEX idx_verstrekking__registratiedatum;
CREATE INDEX idx_verstrekking__registratiedatum ON verstrekking USING btree (registratiedatum DESC NULLS LAST, verstrekking_datum);

-- Index: fkx_verstrekking__patient_id
-- DROP INDEX fkx_verstrekking__patient_id;
CREATE INDEX fkx_verstrekking__patient_id ON wh.verstrekking USING btree (patient_id);
-- Index: fkx_verstrekking__specialisme
-- DROP INDEX fkx_verstrekking__specialisme;
CREATE INDEX fkx_verstrekking__specialisme ON wh.verstrekking USING btree (specialisme_id);


-- DROP POLICY IF EXISTS rls_verstrekking ON wh.verstrekking;
ALTER TABLE wh.verstrekking ENABLE ROW LEVEL SECURITY;
CREATE POLICY rls_verstrekking ON wh.verstrekking USING (pg_has_role(SESSION_user,a_naam,'MEMBER'));


/* *********************************************************************************** */


﻿SET search_path TO "P_WH",public;
SHOW search_path;
CREATE TABLE "P_WH".test_2014_03 () INHERITS ("P_WH".test);

SELECT *
FROM test
WHERE tijdstip < '2014-02-01'::date;


ALTER TABLE test CHECK CONSTRAINT (tijdstip >= '2014-03-01 00:00:00'::datetime AND tijdstip < '2014-04-01 00:00:00'::datetime);

CREATE TABLE "P_WH"."test_2014_04" (CHECK ( tijdstip >= '2014-04-01 00:00:00'::datetime AND tijdstip < '2014-05-01 00:00:00'::datetime)) INHERITS ("P_WH"."test");


SELECT * FROM test;
SELECT LOCALTIMESTAMP(0) AS tijdstip;
SELECT LOCALTIMESTAMP(0)::date + TIME '00:00' AS tijdstip, TO_CHAR(LOCALTIMESTAMP(0)::date,'YYYY_MM');


SELECT DATE_TRUNC('MONTH', LOCALTIMESTAMP(0)) + INTERVAL '1 MONTH' + TIME '00:00' AS volgendeMaand;
SELECT DATE_TRUNC('MONTH', LOCALTIMESTAMP(0)) + TIME '00:00' AS huidigeMaand;



SELECT (DATE_TRUNC('MONTH', LOCALTIMESTAMP(0)::date) + TIME '00:00')::timestamp;


INSERT INTO test_2014_03 (tijdstip)
VALUES ('2014-03-12 01:23:45');

SELECT c.*, n.*
FROM   pg_catalog.pg_class c
JOIN   pg_catalog.pg_namespace n
ON n.oid = c.relnamespace
WHERE  c.relkind = 'r'
AND    c.relname = 'test'
AND    n.nspname = 'P_WH';

EXPLAIN
SELECT *
FROM "P_WH"."test";

 /****************************************************************************************************************/

CREATE OR REPLACE FUNCTION "P_WH".auto_partition_test()
RETURNS TRIGGER AS
$BODY$
DECLARE
_tablename text;
_sdDT DATE;
_startdate text;
_enddate text;
_result record;
BEGIN
--Takes the current inbound "time" value and determines when midnight is for the given date
_sdDT= NEW."tijdstip"::date;
_startdate := (DATE_TRUNC('MONTH', _sdDT) + TIME '00:00')::TIMESTAMP;
_enddate := (DATE_TRUNC('MONTH', _sdDT) + INTERVAL '1 MONTH' + TIME '00:00')::TIMESTAMP;
_tablename := CONCAT('test_',TO_CHAR(_sdDT,'YYYY_MM'));

-- Check if the partition needed for the current record exists
PERFORM 1
FROM   pg_catalog.pg_class c
JOIN   pg_catalog.pg_namespace n
ON n.oid = c.relnamespace
WHERE  c.relkind = 'r'
AND    c.relname = _tablename
AND    n.nspname = 'P_WH';

-- If the partition needed does not yet exist, then we create it:
-- Note that || is string concatenation (joining two strings to make one)
IF NOT FOUND THEN
	EXECUTE 'CREATE TABLE "P_WH".' || quote_ident(_tablename) || ' (CHECK ( "tijdstip" >= ' || quote_literal(_startdate) || ' AND "tijdstip" < ' || quote_literal(_enddate) || ')) INHERITS ("P_WH"."test")';

	-- Table permissions are not inherited from the parent.
	-- If permissions change on the master be sure to change them on the child also.
	EXECUTE 'ALTER TABLE "P_WH".' || quote_ident(_tablename) || ' OWNER TO postgres';
	--EXECUTE 'GRANT ALL ON TABLE "P_WH".' || quote_ident(_tablename) || ' TO my_role';

	-- Indexes are defined per child, so we assign a default index that uses the partition columns
	EXECUTE 'CREATE INDEX ' || quote_ident(CONCAT(_tablename,'_indx1')) || ' ON "P_WH".' || quote_ident(_tablename) || ' (tijdstip, "test_ID")';
END IF;

-- Insert the current record into the correct partition, which we are sure will now exist.
EXECUTE 'INSERT INTO "P_WH".' || quote_ident(_tablename) || ' VALUES ($1.*)' USING NEW;
RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql;

 /* Hier de feitelijke trigger */
CREATE TRIGGER test_insert_trigger
BEFORE INSERT ON "P_WH".test
FOR EACH ROW EXECUTE PROCEDURE "P_WH".auto_partition_test();

 /* Testje  */
INSERT INTO "P_WH"."test" (tijdstip) VALUES ('2014-01-12 01:23:45');
SELECT * FROM "P_WH"."test";
INSERT INTO "P_WH"."test" (tijdstip) VALUES ('2014-02-12 01:23:45');
INSERT INTO "P_WH"."test" (tijdstip) VALUES ('2014-02-12 02:23:45');
INSERT INTO "P_WH"."test" (tijdstip) VALUES ('2014-03-11 01:23:45');
SELECT * FROM "P_WH"."test";

ALTER TABLE "P_WH"."test" ADD COLUMN "systeem" INTEGER DEFAULT 1;

SELECT *
FROM ONLY "P_WH"."test_2014_02";

ALTER TABLE "P_WH".test
  ADD CONSTRAINT tijd_systeem UNIQUE(tijdstip, systeem);

 INSERT INTO "P_WH"."test" (tijdstip,systeem) VALUES ('2014-03-11 01:23:45',2);
 INSERT INTO "P_WH"."test" (tijdstip,systeem) VALUES ('2014-03-11 01:23:45',3);
 INSERT INTO "P_WH"."test" (tijdstip,systeem) VALUES ('2014-03-11 01:23:45',4);
 INSERT INTO "P_WH"."test" (tijdstip,systeem) VALUES ('2014-03-11 01:23:45',5);
 INSERT INTO "P_WH"."test" (tijdstip,systeem) VALUES ('2014-03-11 01:23:45',6);
 INSERT INTO "P_WH"."test" (tijdstip,systeem) VALUES ('2014-03-11 01:23:45',7);
 INSERT INTO "P_WH"."test" (tijdstip,systeem) VALUES ('2014-03-11 01:23:45',8);
SELECT * FROM "P_WH"."test";

 INSERT INTO "P_WH"."test" (tijdstip,systeem) VALUES ('2014-03-11 01:23:45',8);
