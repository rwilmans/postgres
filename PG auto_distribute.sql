/*
-- SEQUENCE: wh.seq_voorschrijver_id
-- DROP SEQUENCE IF EXISTS wh.seq_voorschrijver_id;
CREATE SEQUENCE IF NOT EXISTS wh.seq_voorschrijver_id
    INCREMENT 1
    START 1
    MINVALUE 0
    MAXVALUE 9223372036854775807
;
ALTER SEQUENCE wh.seq_voorschrijver_id OWNER TO dwh_owner;

-- Table: wh.voorschrijver
-- DROP TABLE IF EXISTS wh.voorschrijver CASCADE;
CREATE TABLE wh.voorschrijver(
    voorschrijver_id INTEGER NOT NULL DEFAULT NEXTVAL('wh.seq_voorschrijver_id'),
	agb_code INTEGER NOT NULL,
	meta_valid_from TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
	meta_is_active BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT pk_voorschrijver PRIMARY KEY (voorschrijver_id)
);

ALTER TABLE wh.voorschrijver OWNER to dwh_owner;
-- GRANT SELECT ON TABLE wh.voorschrijver TO dwh_readonly;

-- Table: wh.voorschrijver_hist
-- DROP TABLE wh.voorschrijver_hist;
CREATE TABLE wh.voorschrijver_hist(
	CONSTRAINT pk_voorschrijver_hist PRIMARY KEY (voorschrijver_id, meta_valid_from),
    CONSTRAINT fk_voorschrijver_hist__voorschrijver FOREIGN KEY (voorschrijver_id) REFERENCES wh.voorschrijver (voorschrijver_id) ON DELETE CASCADE
) INHERITS (wh.voorschrijver)
;
ALTER TABLE wh.voorschrijver_hist OWNER TO dwh_owner;
-- GRANT SELECT ON TABLE wh.voorschrijver_hist TO dwh_readonly;


-- Table: wh.voorschrijver_futr
-- DROP TABLE wh.voorschrijver_futr;
CREATE TABLE wh.voorschrijver_futr(
	CONSTRAINT pk_voorschrijver_futr PRIMARY KEY (voorschrijver_id, meta_valid_from),
    CONSTRAINT fk_voorschrijver_futr__voorschrijver FOREIGN KEY (voorschrijver_id) REFERENCES wh.voorschrijver (voorschrijver_id) ON DELETE CASCADE
) INHERITS (wh.voorschrijver)
;
ALTER TABLE wh.voorschrijver_futr OWNER TO dwh_owner;
-- GRANT SELECT ON TABLE wh.voorschrijver_futr TO dwh_readonly;


*/

/* 
CREATE OR REPLACE FUNCTION generic.test_distribute()
RETURNS BOOL AS
$fnc_body$
DECLARE
	_id_column CHARACTER VARYING; -- De naam van de column die de sleutel bevat voor deze tabel
	_id_column_d1 CHARACTER VARYING; -- _id_column, elk element voorafgegaan door '$1.'
	_id_column_c CHARACTER VARYING; -- _id_column, elk element voorafgegaan door 'c.'
	_id_column_f CHARACTER VARYING; -- _id_column, elk element voorafgegaan door 'f.'

	-- Voor opbouw UPDATE-query
    _columns TEXT;
    _columns_new TEXT;
    _columns_futr TEXT;

	_xid INT := txid_current();
	
	_key_ts CHARACTER VARYING;
	TG_TABLE_NAME CHARACTER VARYING := 'voorschrijver';
	TG_TABLE_SCHEMA CHARACTER VARYING := 'wh';

BEGIN
	-- Obtain the ID-columns (so, the primary key of the 'current'-table)
	SELECT 	QUOTE_LITERAL(STRING_AGG(k.column_name,',' ORDER BY col.ordinal_position)),
			QUOTE_LITERAL(STRING_AGG(CONCAT('$1.',k.column_name),',' ORDER BY col.ordinal_position)),
			QUOTE_LITERAL(STRING_AGG(CONCAT('c.',k.column_name),',' ORDER BY col.ordinal_position)),
			QUOTE_LITERAL(STRING_AGG(CONCAT('f.',k.column_name),',' ORDER BY col.ordinal_position))
			INTO
			_id_column,
			_id_column_d1,
			_id_column_c,
			_id_column_f
	FROM 	information_schema.table_constraints AS c,
			information_schema.key_column_usage AS k,
			information_schema.columns AS col
	WHERE 	c.table_name = QUOTE_IDENT(TG_TABLE_NAME) AND
			c.table_schema = QUOTE_IDENT (TG_TABLE_SCHEMA) AND
			c.constraint_type = 'PRIMARY KEY' AND
			c.constraint_name = k.constraint_name AND
			c.table_name = k.table_name AND
			c.table_schema = k.table_schema AND
			k.column_name = col.column_name AND
			c.table_name = col.table_name AND
			c.table_schema = col.table_schema AND
			col.data_type = 'integer'
	;

	-- Obtain the time-column (part of the PK in the ..._hist-table)
	SELECT 	k.column_name INTO _key_ts
	FROM 	information_schema.table_constraints AS c,
			information_schema.key_column_usage AS k,
			information_schema.columns AS col
	WHERE 	c.table_name = CONCAT(TG_TABLE_NAME,'_hist') AND
			c.table_schema = TG_TABLE_SCHEMA AND
			c.constraint_type = 'PRIMARY KEY' AND
			c.constraint_name = k.constraint_name AND
			c.table_name = k.table_name AND
			c.table_schema = k.table_schema AND
			k.column_name = col.column_name AND
			c.table_name = col.table_name AND
			c.table_schema = col.table_schema AND
			col.data_type ~* 'timestamp|date' AND
			_id_column !~ k.column_name 
	;
	
	RAISE NOTICE 'Tijdsvar: %', _key_ts;

	-- The rest of the columns
	SELECT 	QUOTE_LITERAL(STRING_AGG(QUOTE_IDENT(attname), ', ')),
			QUOTE_LITERAL(STRING_AGG('($1).' || QUOTE_IDENT(attname), ', ')),
			QUOTE_LITERAL(STRING_AGG('f.' || QUOTE_IDENT(attname), ', '))
			INTO 	_columns,
					_columns_new,
					_columns_futr
	FROM   pg_catalog.pg_attribute
	WHERE  	attrelid = CONCAT(TG_TABLE_SCHEMA,'.',TG_TABLE_NAME)::regclass AND
			attisdropped = FALSE AND
			attnum > 0 AND
			_id_column !~* attname AND -- Not one of the ID-columns
			_key_ts !~* attname -- and not the time-indicators
	;
	
	RAISE NOTICE '_columns: %', _columns;

	RETURN TRUE;
END;
$fnc_body$
LANGUAGE plpgsql
-- SECURITY DEFINER
SET search_path = generic, wh, pg_temp
;

SELECT generic.test_distribute();

*/


 -- --------------------------------------------------------------------------------------------------------
 -- --------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION generic.pre_auto_distribute()
RETURNS TRIGGER AS
$fnc_body$
DECLARE
	_id_column CHARACTER VARYING; -- Name(s) of the column(s) defining the key for this table
	_id_column_d1 CHARACTER VARYING; -- _id_column but every element prefixed with '$1.'
	_id_column_c CHARACTER VARYING; -- _id_column but every element prefixed with 'c.'
	_id_column_f CHARACTER VARYING; -- _id_column but every element prefixed with 'f.'
	_key_ts CHARACTER VARYING; -- Name of the time-column

	-- For construction UPDATE-query
    _columns TEXT;
    _columns_new TEXT;
    _columns_futr TEXT;
	_update_set_string CHARACTER VARYING;

	_xid INT := txid_current();
	
	
BEGIN
	-- Obtain the ID-columns (so, the primary key of the 'current'-table)
	SELECT 	QUOTE_LITERAL(STRING_AGG(k.column_name,',' ORDER BY col.ordinal_position)),
			QUOTE_LITERAL(STRING_AGG(CONCAT('$1.',k.column_name),',' ORDER BY col.ordinal_position)),
			QUOTE_LITERAL(STRING_AGG(CONCAT('c.',k.column_name),',' ORDER BY col.ordinal_position)),
			QUOTE_LITERAL(STRING_AGG(CONCAT('f.',k.column_name),',' ORDER BY col.ordinal_position))
			INTO
			_id_column,
			_id_column_d1,
			_id_column_c,
			_id_column_f
	FROM 	information_schema.table_constraints AS c,
			information_schema.key_column_usage AS k,
			information_schema.columns AS col
	WHERE 	c.table_name = QUOTE_IDENT(TG_TABLE_NAME) AND
			c.table_schema = QUOTE_IDENT (TG_TABLE_SCHEMA) AND
			c.constraint_type = 'PRIMARY KEY' AND
			c.constraint_name = k.constraint_name AND
			c.table_name = k.table_name AND
			c.table_schema = k.table_schema AND
			k.column_name = col.column_name AND
			c.table_name = col.table_name AND
			c.table_schema = col.table_schema AND
			col.data_type = 'integer'
	;
	
	EXECUTE FORMAT ('SET LOCAL _%s._id_column = %s',_xid,_id_column);
	EXECUTE FORMAT ('SET LOCAL _%s._id_column_d1 = %s',_xid,_id_column_d1);
	EXECUTE FORMAT ('SET LOCAL _%s._id_column_c = %s',_xid,_id_column_c);
	EXECUTE FORMAT ('SET LOCAL _%s._id_column_f = %s',_xid,_id_column_f);

	-- Obtain the time-column (part of the PK in the ..._hist-table)
	SELECT 	k.column_name INTO _key_ts
	FROM 	information_schema.table_constraints AS c,
			information_schema.key_column_usage AS k,
			information_schema.columns AS col
	WHERE 	c.table_name = CONCAT(TG_TABLE_NAME,'_hist') AND
			c.table_schema = TG_TABLE_SCHEMA AND
			c.constraint_type = 'PRIMARY KEY' AND
			c.constraint_name = k.constraint_name AND
			c.table_name = k.table_name AND
			c.table_schema = k.table_schema AND
			k.column_name = col.column_name AND
			c.table_name = col.table_name AND
			c.table_schema = col.table_schema AND
			col.data_type ~* 'timestamp|date' AND
			_id_column !~ k.column_name 
	;
	EXECUTE FORMAT ('SET LOCAL _%s._key_ts = %s',_xid,_key_ts);

	-- The rest of the columns
	SELECT 	QUOTE_LITERAL(STRING_AGG(QUOTE_IDENT(attname), ', ')),
			QUOTE_LITERAL(STRING_AGG('($1).' || QUOTE_IDENT(attname), ', ')),
			QUOTE_LITERAL(STRING_AGG('f.' || QUOTE_IDENT(attname), ', '))
			INTO 	_columns,
					_columns_new,
					_columns_futr
	FROM   pg_catalog.pg_attribute
	WHERE  	attrelid = CONCAT(TG_TABLE_SCHEMA,'.',TG_TABLE_NAME)::regclass AND
			attisdropped = FALSE AND
			attnum > 0 AND
			_id_column !~* attname AND -- Not one of the ID-columns
			_key_ts !~* attname -- and not the time-indicators
	;

	EXECUTE FORMAT ('SET LOCAL _%s._columns = %s',_xid,_columns);
	EXECUTE FORMAT ('SET LOCAL _%s._columns_new = %s',_xid,_columns_new);
	EXECUTE FORMAT ('SET LOCAL _%s._columns_futr = %s',_xid,_columns_futr);
	
	-- If there's only ONE column to update, then a SET (x) = (y) cannot be used, so:
	IF REGEXP_COUNT(_columns, ',' ) > 0
	THEN 
		_update_set_string = '(%s) = (%s)';
	ELSE 
		_update_set_string = '%s = %s';
	END IF;
	EXECUTE FORMAT ('SET LOCAL _%s._update_set_string = ''%s''',_xid,_update_set_string);

	-- ... and allow the rest of the triggers to do their jobs
	RETURN NEW;
END;
$fnc_body$
LANGUAGE plpgsql
-- SECURITY DEFINER
SET search_path = 'generic, wh, pg_temp, "$user"'
;

ALTER FUNCTION generic.pre_auto_distribute OWNER TO dwh_owner;
-- GRANT EXECUTE ON FUNCTION generic.prep_auto_distribute TO dwh_owner, web_app_owners, pd_admin_users;
;

 -- --------------------------------------------------------------------------------------------------------
 -- --------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION generic.auto_distribute()
RETURNS TRIGGER AS
$fnc_body$
DECLARE
	-- Current (this!) transaction-ID
	_xid CHARACTER VARYING := CONCAT('_',txid_current(),'.');

     -- INSERT-values:
    _ID_values CHARACTER VARYING;
    _input_meta_valid_from TIMESTAMP WITH TIME ZONE;

	_id_column CHARACTER VARYING := CURRENT_SETTING(CONCAT(_xid,'_id_column'));  -- Name(s) of the column(s) defining the key for this table
	_id_column_d1 CHARACTER VARYING := CURRENT_SETTING(CONCAT(_xid,'_id_column_d1')); -- _id_column but every element prefixed with '$1.'
	_id_column_c CHARACTER VARYING := CURRENT_SETTING(CONCAT(_xid,'_id_column_c')); -- _id_column but every element prefixed with 'c.'
	_id_column_f CHARACTER VARYING := CURRENT_SETTING(CONCAT(_xid,'_id_column_f')); -- _id_column but every element prefixed with 'f.'
	_key_ts CHARACTER VARYING := CURRENT_SETTING(CONCAT(_xid,'_key_ts')); -- Name of the time-column

	-- For construction UPDATE-query
    _columns TEXT := CURRENT_SETTING(CONCAT(_xid,'_columns'));
    _columns_new TEXT := CURRENT_SETTING(CONCAT(_xid,'_columns_new'));
    _columns_futr TEXT := CURRENT_SETTING(CONCAT(_xid,'_columns_futr'));
	_update_set_string CHARACTER VARYING := CURRENT_SETTING(CONCAT(_xid,'_update_set_string'));

	-- What date is currently registered as 'current'
    _current_meta_valid_from TIMESTAMP WITH TIME ZONE;
	_exists BOOLEAN; 
	_target_table TEXT; -- The TARGET table
	_results RECORD;
	_id_found INTEGER;
	
BEGIN
	-- Obtain the meta_valid_from as inputted, or its default value; if still NULL, then 'now':
	EXECUTE FORMAT ('SELECT COALESCE($1.%I,CURRENT_TIMESTAMP)', _key_ts) USING NEW INTO _input_meta_valid_from;

	-- To test if there are any NULLs in a ID-column:
	EXECUTE FORMAT ('SELECT COUNT(*)::INT aant_null FROM UNNEST(ARRAY[%s]::INTEGER[]) AS i WHERE i IS NULL',_id_column_d1) USING NEW INTO _id_found;
-- 	RAISE NOTICE '_id_found: (%)', _id_found;

	IF _id_found > 0 THEN
		-- If an ID-column (primary key) has a default value ('nextval'), this can never be the case... but:
		-- So, there's a primary key (ID-column) that has NULL for value.
		-- As a result, this should be a new record and thus simply be added:
		-- RAISE NOTICE 'Het id (%) bevat een NULL-waarde', _ID;
		RETURN NEW;
	ELSE
		-- Determine the values of the ID-columns
		EXECUTE FORMAT('SELECT CONCAT_WS('','',%s)',_id_column_d1) USING NEW INTO _ID_values;
		--RAISE NOTICE '(%)=(%)',_id_kolom_d1, _ID;

		-- Does this record already exist in the ('current'-) table
		-- Obtain the value of the 'current' time-column
		EXECUTE FORMAT('SELECT %I FROM ONLY %I.%I WHERE (%s) = (%s)', _key_ts, TG_TABLE_SCHEMA, TG_TABLE_NAME, _id_column, _ID_values) INTO _current_meta_valid_from;

		IF _current_meta_valid_from IS NULL THEN
			-- Nope, does not exist - simply add record: 
			RETURN NEW;
		ELSE
			-- ------------------------------------------------------------------------------------------------
			-- Yes, this ID is already registered in 'current' table
			-- Determine whether historical, current or future and whether to be inserted or updated 
			IF _current_meta_valid_from::TIMESTAMP WITH TIME ZONE > _input_meta_valid_from::TIMESTAMP WITH TIME ZONE
			THEN	_target_table = CONCAT(TG_TABLE_SCHEMA,'.',TG_TABLE_NAME,'_hist'); -- This is either an update or insert to the HIST table
			ELSEIF _current_meta_valid_from::TIMESTAMP WITH TIME ZONE = _input_meta_valid_from::TIMESTAMP WITH TIME ZONE THEN
					_target_table = CONCAT(TG_TABLE_SCHEMA,'.',TG_TABLE_NAME); -- An update on the 'current' table
			ELSE	_target_table = CONCAT(TG_TABLE_SCHEMA,'.',TG_TABLE_NAME,'_futr'); -- -- This is either an update or insert to the FUTR table
			END IF;

			-- Does the record already exists (with the given date) in the appropriate table?
			_exists = FALSE;
			EXECUTE FORMAT('SELECT TRUE 
						   	FROM %s 
						   	WHERE 	(%s) = (%s) AND 
						   			%I = %L', 
						   	_target_table, 
						   	_id_column, _ID_values, 
						   	_key_ts, _input_meta_valid_from
						  	) INTO _exists;

			IF _exists = TRUE THEN
				-- Yes, so UPDATE
				EXECUTE FORMAT('UPDATE %s 
							   	SET ' || _update_set_string ||  ' 
							   	WHERE 	(%s) = (%s) AND 
							   			%I = %L', 
							   	_target_table, 
							   	_columns, _columns_new, 
							   	_id_column, _ID_values, 
							   	_key_ts, _input_meta_valid_from
							  	) USING NEW;
			ELSE
				-- Not yet, so ADD
				EXECUTE FORMAT('INSERT INTO %s VALUES ($1.*)', _target_table) USING NEW;
			END IF;
			-- ------------------------------------------------------------------------------------------------
		END IF;
		RETURN NULL; -- Stop processing anything else...
	END IF; -- There is an ID with NULL-value
END;
$fnc_body$
LANGUAGE plpgsql
--SECURITY DEFINER
SET search_path = 'generic, wh, pg_temp, "$user"'
;

ALTER FUNCTION generic.auto_distribute OWNER TO dwh_owner;
-- GRANT EXECUTE ON FUNCTION generic.auto_distribute TO dwh_owner, web_app_owners, web_app_default_users, pd_admin_users;
;

 -- --------------------------------------------------------------------------------------------------------
 -- --------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION generic.post_auto_distribute()
RETURNS TRIGGER AS
$fnc_body$
DECLARE
	-- Current (this!) transaction-ID
	_xid CHARACTER VARYING := (SELECT CONCAT('_',txid_current(),'.'));

	_id_column CHARACTER VARYING := CURRENT_SETTING(CONCAT(_xid,'_id_column'));  -- Name(s) of the column(s) defining the key for this table
	_id_column_c CHARACTER VARYING := CURRENT_SETTING(CONCAT(_xid,'_id_column_c')); -- _id_column but every element prefixed with 'c.'
	_id_column_f CHARACTER VARYING := CURRENT_SETTING(CONCAT(_xid,'_id_column_f')); -- _id_column but every element prefixed with 'f.'
	_key_ts CHARACTER VARYING := CURRENT_SETTING(CONCAT(_xid,'_key_ts')); -- Name of the time-column

	_columns CHARACTER VARYING := CURRENT_SETTING(CONCAT(_xid,'_columns'));
	_columns_futr CHARACTER VARYING := CURRENT_SETTING(CONCAT(_xid,'_columns_futr'));
	_update_set_string CHARACTER VARYING := CURRENT_SETTING(CONCAT(_xid,'_update_set_string'));
	
	_full_table CHARACTER VARYING := CONCAT(TG_TABLE_SCHEMA, '.', TG_TABLE_NAME);
	_results RECORD;

BEGIN
	-- Find any records in the FUTR-table with TIME-column <= 'now'...
	-- If there are any, then move as many records to 'current' or HIST-table as possible
	FOR _results IN
		EXECUTE FORMAT ('	SELECT CONCAT_WS('','',%s) AS _id, %I AS _time_column
							FROM %s
							WHERE %I <= CURRENT_TIMESTAMP
							ORDER BY %I ASC', 
							_id_column, _key_ts, 
							CONCAT(_full_table,'_futr'), 
							_key_ts,
					   		_key_ts)
	LOOP
		-- For every legitimate future record that has to be moved to the 'current' table,
		-- copy that 'current' record into the HIST-table:
		EXECUTE FORMAT ('	INSERT INTO %s
							SELECT *
							FROM ONLY %s
							WHERE (%s) = (%s)',
							CONCAT(_full_table,'_hist'),
							_full_table,
							_id_column, _results._id);

		-- ... and update the 'current' table (to reflect the FUTR-situation (which is now the 'current'!))
-- 		RAISE NOTICE '_update_set_string : %', _update_set_string;
		EXECUTE FORMAT('UPDATE ONLY %s AS c 
						SET ' || _update_set_string ||  ', 
							%I = %L
						FROM (	SELECT *
								FROM %s
								WHERE 	(%s) = (%s) AND
										%I = %L) AS f
						WHERE 	(%s) = (%s) AND
								(%s) = (%s)',
						_full_table,
					   	_columns, _columns_futr,
						_key_ts, _results._time_column,
						CONCAT(_full_table,'_futr'),
						_id_column, _results._id,
						_key_ts, _results._time_column,
						_id_column_c, _id_column_f,
						_id_column_c, _results._id
						);

		-- ... and delete those records from the FUTR-table, as they are now the 'current' situation!
		EXECUTE FORMAT ('	DELETE
							FROM %s
							WHERE 	%I = %L AND
									(%s) = (%s)',
							CONCAT(_full_table,'_futr'),
							_key_ts, _results._time_column,
							_id_column, _results._id);

	END LOOP;

RETURN NULL;
END;
$fnc_body$
LANGUAGE plpgsql
SET search_path = 'generic, wh, pg_temp, "$user"'
;
ALTER FUNCTION generic.post_auto_distribute OWNER TO dwh_owner;

 -- --------------------------------------------------------------------------------------------------------
 -- --------------------------------------------------------------------------------------------------------
DROP SEQUENCE IF EXISTS wh.seq_voorschrijver_id;
CREATE SEQUENCE IF NOT EXISTS wh.seq_voorschrijver_id
    INCREMENT 1
    START 1
    MINVALUE 0
    MAXVALUE 9223372036854775807
;
ALTER SEQUENCE wh.seq_voorschrijver_id OWNER TO dwh_owner;

-- DROP TABLE IF EXISTS wh.voorschrijver CASCADE;
CREATE TABLE wh.voorschrijver(
    voorschrijver_id BIGINT NOT NULL DEFAULT NEXTVAL('wh.seq_voorschrijver_id'),
	agb_code INTEGER NOT NULL,
	meta_valid_from TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
	meta_is_active BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT pk_voorschrijver PRIMARY KEY (voorschrijver_id)
);
ALTER TABLE wh.voorschrijver OWNER to dwh_owner;

CREATE TABLE wh.voorschrijver_hist(
	CONSTRAINT pk_voorschrijver_hist PRIMARY KEY (voorschrijver_id, meta_valid_from),
    CONSTRAINT fk_voorschrijver_hist__voorschrijver FOREIGN KEY (voorschrijver_id) REFERENCES wh.voorschrijver (voorschrijver_id) ON DELETE CASCADE
) INHERITS (wh.voorschrijver)
;
ALTER TABLE wh.voorschrijver_hist OWNER TO dwh_owner;

CREATE TABLE wh.voorschrijver_futr(
	CONSTRAINT pk_voorschrijver_futr PRIMARY KEY (voorschrijver_id, meta_valid_from),
    CONSTRAINT fk_voorschrijver_futr__voorschrijver FOREIGN KEY (voorschrijver_id) REFERENCES wh.voorschrijver (voorschrijver_id) ON DELETE CASCADE
) INHERITS (wh.voorschrijver)
;
ALTER TABLE wh.voorschrijver_futr OWNER TO dwh_owner;



DROP TRIGGER IF EXISTS trg_pre_voorschrijver ON wh.voorschrijver;
DROP TRIGGER IF EXISTS trg_voorschrijver ON wh.voorschrijver;
DROP TRIGGER IF EXISTS trg_post_voorschrijver ON wh.voorschrijver;

CREATE TRIGGER trg_pre_voorschrijver BEFORE INSERT ON wh.voorschrijver FOR EACH STATEMENT EXECUTE PROCEDURE generic.pre_auto_distribute();
CREATE TRIGGER trg_voorschrijver BEFORE INSERT ON wh.voorschrijver FOR EACH ROW EXECUTE PROCEDURE generic.auto_distribute();
CREATE TRIGGER trg_post_voorschrijver AFTER INSERT ON wh.voorschrijver FOR EACH STATEMENT EXECUTE PROCEDURE generic.post_auto_distribute();


TRUNCATE TABLE wh.voorschrijver;

INSERT INTO wh.voorschrijver (voorschrijver_id, agb_code, meta_valid_from) VALUES (10, 111,'2022/01/01');
SELECT * FROM wh.voorschrijver;
SELECT * FROM ONLY wh.voorschrijver;

INSERT INTO wh.voorschrijver (voorschrijver_id, agb_code, meta_valid_from) VALUES (10,112,'2022/01/01');
SELECT * FROM wh.voorschrijver;
SELECT * FROM ONLY wh.voorschrijver;

INSERT INTO wh.voorschrijver (voorschrijver_id, agb_code, meta_valid_from) VALUES (10,222,'2022/02/01');
SELECT * FROM wh.voorschrijver;
SELECT * FROM ONLY wh.voorschrijver;
SELECT * FROM ONLY wh.voorschrijver_hist;

INSERT INTO wh.voorschrijver (voorschrijver_id, agb_code, meta_valid_from) VALUES (10,223,'2022/01/01');
SELECT * FROM wh.voorschrijver;
SELECT * FROM ONLY wh.voorschrijver;
SELECT * FROM ONLY wh.voorschrijver_hist;
SELECT * FROM ONLY wh.voorschrijver_futr;

INSERT INTO wh.voorschrijver (voorschrijver_id, agb_code, meta_valid_from) VALUES (10,444,'2024/01/01');
SELECT * FROM wh.voorschrijver;
SELECT * FROM ONLY wh.voorschrijver;
SELECT * FROM ONLY wh.voorschrijver_hist;
SELECT * FROM ONLY wh.voorschrijver_futr;

INSERT INTO wh.voorschrijver (voorschrijver_id, agb_code, meta_valid_from) VALUES (10,445,'2024/01/01');
SELECT * FROM ONLY wh.voorschrijver_futr;

DROP TABLE IF EXISTS wh.voorschrijver CASCADE;
DROP SEQUENCE IF EXISTS wh.seq_voorschrijver_id;

 -- --------------------------------------------------------------------------------------------------------
 -- --------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS wh.de_een;
CREATE TABLE wh.de_een (de_een_id INT NOT NULL PRIMARY KEY);
INSERT INTO wh.de_een(de_een_id) VALUES (10);

DROP TABLE IF EXISTS wh.ander;
CREATE TABLE wh.ander (ander_id INT NOT NULL PRIMARY KEY);
INSERT INTO wh.ander(ander_id) VALUES (11);

-- DROP TABLE IF EXISTS wh.inbetween CASCADE;
CREATE TABLE wh.inbetween(
    de_een_id INTEGER NOT NULL,
	ander_id INTEGER NOT NULL,
	meta_valid_from TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
	meta_is_active BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT pk_inbetween PRIMARY KEY (de_een_id, ander_id), 
	CONSTRAINT fk_inbetween__de_een FOREIGN KEY (de_een_id) REFERENCES wh.de_een (de_een_id) ON DELETE CASCADE,
	CONSTRAINT fk_inbetween__ander FOREIGN KEY (ander_id) REFERENCES wh.ander (ander_id) ON DELETE CASCADE
);
ALTER TABLE wh.inbetween OWNER to dwh_owner;

-- DROP TABLE wh.inbetween_hist;
CREATE TABLE wh.inbetween_hist(
	CONSTRAINT pk_inbetween_hist PRIMARY KEY (de_een_id, meta_valid_from),
    CONSTRAINT fk_inbetween_hist__inbetween FOREIGN KEY (de_een_id, ander_id) REFERENCES wh.inbetween (de_een_id, ander_id) ON DELETE CASCADE
) INHERITS (wh.inbetween)
;
ALTER TABLE wh.inbetween_hist OWNER TO dwh_owner;

-- DROP TABLE wh.voorschrijver_futr;
CREATE TABLE wh.inbetween_futr(
	CONSTRAINT pk_inbetween_futr PRIMARY KEY (de_een_id, meta_valid_from),
    CONSTRAINT fk_inbetween_futr__inbetween FOREIGN KEY (de_een_id, ander_id) REFERENCES wh.inbetween (de_een_id, ander_id) ON DELETE CASCADE
) INHERITS (wh.inbetween)
;
ALTER TABLE wh.inbetween_futr OWNER TO dwh_owner;

 -- -----------------------------------------------------
DROP TRIGGER IF EXISTS trg_pre_inbetween ON wh.inbetween;
DROP TRIGGER IF EXISTS trg_inbetween ON wh.inbetween;
DROP TRIGGER IF EXISTS trg_post_inbetween ON wh.inbetween;
CREATE TRIGGER trg_pre_inbetween BEFORE INSERT ON wh.inbetween FOR EACH STATEMENT EXECUTE PROCEDURE generic.pre_auto_distribute();
CREATE TRIGGER trg_inbetween BEFORE INSERT ON wh.inbetween FOR EACH ROW EXECUTE PROCEDURE generic.auto_distribute();
CREATE TRIGGER trg_post_inbetween AFTER INSERT ON wh.inbetween FOR EACH STATEMENT EXECUTE PROCEDURE generic.post_auto_distribute();

 -- -----------------------------------------------------
TRUNCATE TABLE wh.inbetween;
INSERT INTO wh.inbetween (de_een_id, ander_id, meta_valid_from, meta_is_active) VALUES (10,11, '2023/01/01', TRUE);
SELECT * FROM wh.inbetween;
SELECT * FROM ONLY wh.inbetween;
SELECT * FROM ONLY wh.inbetween_hist;
SELECT * FROM ONLY wh.inbetween_futr;

INSERT INTO wh.inbetween (de_een_id, ander_id, meta_valid_from, meta_is_active) VALUES (10,11, '2023/01/02', FALSE);
SELECT * FROM wh.inbetween;
SELECT * FROM ONLY wh.inbetween;
SELECT * FROM ONLY wh.inbetween_hist;
SELECT * FROM ONLY wh.inbetween_futr;

DROP TABLE IF EXISTS wh.inbetween CASCADE;
DROP TABLE IF EXISTS wh.de_een;
DROP TABLE IF EXISTS wh.ander;

 -- --------------------------------------------------------------------------------------------------------
 -- --------------------------------------------------------------------------------------------------------
