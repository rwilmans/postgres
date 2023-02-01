 -- ********************************************************************************************
-- DROP PROCEDURE IF EXISTS generic.repartitionize;
CREATE OR REPLACE PROCEDURE generic.repartitionize (org_tabel CHARACTER VARYING = '', org_schema CHARACTER VARYING = '') AS
$FBODY$
DECLARE
_resultaten RECORD;
_tmptable CHARACTER VARYING;
_full_table CHARACTER VARYING;
_default_table CHARACTER VARYING;
_part_key CHARACTER VARYING;

BEGIN
		IF (org_tabel IS NOT NULL AND org_tabel != '') THEN
			IF (org_schema = '') THEN org_schema = 'wh'; END IF;
			_tmptable := CONCAT('_tmp_',org_tabel,'_',TO_CHAR(TRANSACTION_TIMESTAMP(),'YYYYMMDDHH24MISSMSUS'));
			_full_table := CONCAT(org_schema,'.',org_tabel);
			_default_table := CONCAT(org_schema,'.',org_tabel,'_pdefault');

			SELECT col.column_name INTO _part_key
			FROM	(SELECT	partrelid AS oid,
							UNNEST(partattrs) AS col_index
					 FROM pg_catalog.pg_partitioned_table) AS pt,
					pg_catalog.pg_class AS par,
					information_schema.columns col
			WHERE 	pt.oid = par.oid AND
					par.relnamespace::regnamespace::text = col.table_schema AND
					par.relname = col.table_name AND
					pt.col_index = col.ordinal_position AND
					par.relname = org_tabel AND
					par.relnamespace::regnamespace::text = org_schema
			;

			EXECUTE 'LOCK TABLE ' || _full_table || ' IN EXCLUSIVE MODE';
			EXECUTE 'CREATE TEMPORARY TABLE ' || _tmptable || '(LIKE ' || _full_table || ');';
			EXECUTE 'INSERT INTO ' || _tmptable || ' SELECT * FROM ONLY ' || _default_table || ';';
			EXECUTE 'TRUNCATE TABLE ONLY ' || _default_table || ';';

			FOR _resultaten IN
					EXECUTE ('	SELECT in_def.part_table,in_def.startdate, in_def.enddate
								FROM (	SELECT DISTINCT CONCAT (''' || org_tabel || ''',''_p'', TO_CHAR (' || _part_key || ',''YYYYMM''))::NAME AS part_table,
										TO_CHAR(DATE_TRUNC(''MONTH'',' || _part_key || '),''YYYY-MM-DD'') AS startdate,
										TO_CHAR(DATE_TRUNC(''MONTH'',' || _part_key || ' + INTERVAL ''1 MONTH''),''YYYY-MM-DD'') AS enddate
										FROM ' || _tmptable || ') AS in_def
								LEFT JOIN
									(	SELECT relname AS part_table
										FROM   pg_catalog.pg_class c
										JOIN   pg_catalog.pg_namespace n
										ON n.oid = c.relnamespace
										WHERE  	c.relkind = ''r'' AND
												c.relname ~ CONCAT(''^'',''' || org_tabel || ''') AND
												n.nspname =''' || org_schema || ''') AS found
								ON	in_def.part_table = found.part_table
								WHERE found.part_table IS NULL')
			LOOP
				-- Create new partition:
				RAISE NOTICE 'Creating partition: % (% -> %)', _resultaten.part_table, _resultaten.startdate, _resultaten.enddate;
				EXECUTE 'CREATE TABLE ' || org_schema || '.' || _resultaten.part_table || ' PARTITION OF ' || _full_table || ' FOR VALUES FROM (' || quote_literal(_resultaten.startdate) || ') TO (' || quote_literal(_resultaten.enddate) || ');';
			END LOOP;

			EXECUTE 'INSERT INTO ' || _full_table || '  SELECT * FROM ' || _tmptable || ';';
			RAISE NOTICE 'Partitions of table % re-established.', _full_table;
		ELSE
			RAISE NOTICE 'No table given to re-establish partitions for - no action performed.';
		END IF;
END;
$FBODY$
SET search_path = generic, pg_temp
LANGUAGE plpgsql
-- PARALLEL UNSAFE
SECURITY DEFINER
;

ALTER PROCEDURE generic.repartitionize OWNER TO dwh_owner;
-- GRANT EXECUTE ON PROCEDURE generic.repartitionize TO web_app_pluripharm, generiek_admin_users, web_app_default_users, web_app_owners;
-- CALL generic.repartitionize(org_tabel => 'test_auto_partition', org_schema => 'wh');


 -- ********************************************************************************************
CREATE OR REPLACE FUNCTION generic.auto_partition() RETURNS TRIGGER AS
$FBODY$
DECLARE 
	_pid INT;
BEGIN
		SELECT generic.pg_background_launch('CALL generic.repartitionize(org_tabel => ''' || TG_TABLE_NAME || ''', org_schema => '''|| TG_TABLE_SCHEMA || ''');') INTO _pid;
-- 		RAISE NOTICE 'Auto-partitioning, transaction_id: %', _pid;
		RETURN NEW;
END;
$FBODY$
SET search_path = generic, pg_temp LANGUAGE plpgsql SECURITY DEFINER
;
ALTER FUNCTION generic.auto_partition OWNER TO dwh_owner;
 
 -- ********************************************************************************************
CREATE TABLE wh.test_auto_partition (
	test_auto_partition_id SERIAL NOT NULL,
	test_auto_partition_ts TIMESTAMP(6) WITH TIME ZONE NOT NULL DEFAULT STATEMENT_TIMESTAMP(),
	some_text CHARACTER VARYING(3)
) PARTITION BY RANGE (test_auto_partition_ts)
;
ALTER TABLE wh.test_auto_partition OWNER TO dwh_owner;

CREATE TABLE wh.test_auto_partition_pdefault PARTITION OF wh.test_auto_partition DEFAULT;
ALTER TABLE wh.test_auto_partition_pdefault OWNER TO dwh_owner;

 -- ------------------------------------------------------------------

DROP TRIGGER IF EXISTS trg_TAP ON wh.test_auto_partition;
CREATE TRIGGER trg_TAP
AFTER INSERT ON wh.test_auto_partition
FOR EACH STATEMENT 
EXECUTE PROCEDURE generic.auto_partition()
;

 -- ------------------------------------------------------------------

INSERT INTO wh.test_auto_partition (test_auto_partition_id, test_auto_partition_ts, some_text)
VALUES 	(1,'2023/01/01 10:00:00', 'aaa'),
	 	(1,'2023/01/01 11:00:00', 'aab'),
	 	(1,'2023/01/01 12:00:00', 'aba'),
	 	(1,'2023/01/02 12:00:00', 'abb'),
	 	(1,'2023/01/03 12:00:00', 'baa'),
	 	(1,'2023/02/03 12:00:00', 'bab'),
	 	(1,'2023/02/13 13:00:00', 'bba'),
	 	(1,'2023/09/01 10:00:00', 'bbb')
;

/*

CALL generic.repartitionize(org_tabel => 'test_auto_partition', org_schema => 'wh');
SELECT generic.pg_background_launch('CALL generic.repartitionize(org_tabel => ''test_auto_partition'', org_schema => ''wh'');');

SELECT PG_CANCEL_BACKEND(pid) 
FROM pg_stat_activity 
WHERE 	state = 'active' AND 
		pid <> PG_BACKEND_PID();
;

*/


SELECT * 
FROM wh.test_auto_partition
;

SELECT * 
FROM ONLY wh.test_auto_partition_pdefault
;

SELECT * 
FROM ONLY wh.test_auto_partition_p202301
;

SELECT * 
FROM ONLY wh.test_auto_partition_p202302
;

SELECT * 
FROM ONLY wh.test_auto_partition_p202309
;
