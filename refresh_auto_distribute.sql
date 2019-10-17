CREATE OR REPLACE FUNCTION generic.fnc_refresh_distribute (ds_in CHARACTER VARYING DEFAULT '')
RETURNS BOOLEAN AS
$BODY$
DECLARE
	_tabel CHARACTER VARYING;
	_schema CHARACTER VARYING;

	_id_kolom CHARACTER VARYING;
	_id_kolom_c CHARACTER VARYING; -- _id_kolom, elk element voorafgegaan door 'c.'
	_id_kolom_f CHARACTER VARYING; -- _id_kolom, elk element voorafgegaan door 'f.'

	_kolommen CHARACTER VARYING;
	_kolommen_futr CHARACTER VARYING;
	_resultaten RECORD;

BEGIN
	-- Uitzoeken wat het schema is en de tabel
	IF (POSITION('.' IN ds_in) >0) THEN
		_tabel = SPLIT_PART(ds_in,'.',2);
		_schema = SPLIT_PART(ds_in,'.',1);
	ELSE
		SELECT 	c.relname, n.nspname INTO _tabel, _schema
		FROM	pg_catalog.pg_class AS c,
				pg_catalog.pg_namespace AS n,
				(	SELECT 	pad, ROW_NUMBER() OVER() AS volgorde
					FROM (SELECT UNNEST(CURRENT_SCHEMAS(TRUE)) AS pad) AS b
					) AS p
		WHERE 	n.oid = c.relnamespace AND
				c.relname = ds_in AND
				c.relkind = 'r' AND
				n.nspname = p.pad
		ORDER BY p.volgorde
		LIMIT 1;
	END IF;

	-- Uitzoeken wat de '_id'-kolom is (dat is dus de primary key op de 'current'-tabel)
	EXECUTE FORMAT('SELECT 	STRING_AGG(k.column_name,'','' ORDER BY col.ordinal_position),
				   			STRING_AGG(CONCAT(''c.'',k.column_name),'','' ORDER BY col.ordinal_position),
				   			STRING_AGG(CONCAT(''f.'',k.column_name),'','' ORDER BY col.ordinal_position)
					FROM 	information_schema.table_constraints AS c,
							information_schema.key_column_usage AS k,
							information_schema.columns AS col
					WHERE 	c.table_name = ''%s'' AND
							c.table_schema = ''%s'' AND
							c.constraint_type = ''PRIMARY KEY'' AND
							c.constraint_name = k.constraint_name AND
							c.table_name = k.table_name AND
							c.table_schema = k.table_schema AND
							k.column_name = col.column_name AND
							c.table_name = col.table_name AND
							col.data_type = ''integer''
							', _tabel, _schema)
							INTO
							_id_kolom,
							_id_kolom_c,
							_id_kolom_f;

	SELECT 	STRING_AGG(QUOTE_IDENT(attname), ', '),
			STRING_AGG('f.' || QUOTE_IDENT(attname), ', ')
			INTO 	_kolommen,
					_kolommen_futr
	FROM   pg_catalog.pg_attribute
	WHERE  	attrelid = CONCAT(_schema,'.',_tabel)::regclass AND
			attisdropped = FALSE AND
			attnum > 0 AND
			NOT(LOWER(attname) = ANY(STRING_TO_ARRAY(_id_kolom,',')))
	;

	-- Nu de toekomsttabel doorspitten of dat er datums zijn die vóór (of gelijk aan) vandaag zijn...
	-- Zoja, dan zoveel mogelijk vanuit de toekomst-tabel naar de huidige / historie tabel verplaatsen...
	FOR _resultaten IN
		EXECUTE FORMAT ('	SELECT CONCAT_WS('','',%s) AS _id, geldig_vanaf
							FROM %I.%I_futr
							WHERE geldig_vanaf <= CURRENT_DATE
							ORDER BY geldig_vanaf ASC', _id_kolom, _schema, _tabel)
	LOOP
		-- Voor elk legitiem toekomst record dat naar de huidige tabel moet worden verplaatst:
		EXECUTE FORMAT ('	INSERT INTO %I.%I_hist
							SELECT *
							FROM ONLY %I.%I
							WHERE (%s) = (%s)',
							_schema, _tabel,
							_schema, _tabel,
							_id_kolom, _resultaten._id);

		-- ... en de update op de 'huidige' tabel uitvoeren
		EXECUTE FORMAT('UPDATE ONLY %I.%I AS c
						SET (%s) = (%s)
						FROM (	SELECT *
								FROM %I.%I_futr
								WHERE 	%s = %s AND
										geldig_vanaf = %L) AS f
						WHERE 	(%s) = (%s) AND
								(%s) = (%s)',
						_schema, _tabel,
						_kolommen, _kolommen_futr,
						_schema, _tabel,
						_id_kolom, _resultaten._id,
						_resultaten.geldig_vanaf,
						_id_kolom_c, _id_kolom_f,
						_id_kolom_c, _resultaten._id);

		--En de delete in de FUTR-tabel
		EXECUTE FORMAT ('	DELETE
							FROM %I.%I_futr
							WHERE 	geldig_vanaf = %L AND
									(%s) = (%s)',
							_schema, _tabel,
							_resultaten.geldig_vanaf,
							_id_kolom, _resultaten._id);

	END LOOP;

RETURN TRUE;
END;
$BODY$
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = 'generic, pg_temp'
;



CREATE OR REPLACE PROCEDURE generic.fnc_all_refresh_distribute () AS
$fnc_body$
DECLARE
	_resultaten RECORD;
BEGIN
	-- Uitzoeken welke tabellen een trigger hebben die de functie 'auto_distribute' aanroept
	-- En vervolgens itereren over de resultaten
	FOR _resultaten IN
		SELECT 	CONCAT(ns.nspname,'.',c.relname) trg_tabel
				-- pg_get_triggerdef(t.oid) AS definitie,
				-- t.tgname AS trg_name
		FROM 	pg_catalog.pg_trigger AS t,
				pg_catalog.pg_class  AS c,
				pg_catalog.pg_namespace AS ns
		WHERE 	t.tgrelid = c.oid AND
				ns.oid = c.relnamespace AND
				pg_get_triggerdef(t.oid) ~'auto_distribute()'
	LOOP
		-- Voor elke tabel die die functie als trigger heeft :
		EXECUTE FORMAT ('SELECT generic.fnc_refresh_distribute (ds_in =>''%s'')', _resultaten.trg_tabel);
		RAISE NOTICE 'Bijgewerkt naar actuele data: % ', _resultaten.trg_tabel;
	END LOOP;
END;
$fnc_body$
LANGUAGE plpgsql
SECURITY DEFINER
-- SET search_path = administratie, pg_temp
;
