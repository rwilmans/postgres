CREATE OR REPLACE FUNCTION generic.prep_auto_distribute()
RETURNS TRIGGER AS 
$fnc_body$
DECLARE
	_id_kolom CHARACTER VARYING; -- De naam van de kolom die de sleutel bevat voor deze tabel
	_id_kolom_d1 CHARACTER VARYING; -- _id_kolom, elk element voorafgegaan door '$1.'
	_id_kolom_c CHARACTER VARYING; -- _id_kolom, elk element voorafgegaan door 'c.'
	_id_kolom_f CHARACTER VARYING; -- _id_kolom, elk element voorafgegaan door 'f.' 

	-- Voor opbouw UPDATE-query
    _kolommen TEXT;
    _kolommen_new TEXT;
    _kolommen_futr TEXT;
	
	_xid BIGINT;
	
BEGIN
	SELECT txid_current() INTO _xid;
	
	-- Uitzoeken wat de '_id'-kolom is (dat is dus de primary key op de 'current'-tabel)
	SELECT 	QUOTE_LITERAL(STRING_AGG(k.column_name,',' ORDER BY col.ordinal_position)),
			QUOTE_LITERAL(STRING_AGG(CONCAT('$1.',k.column_name),',' ORDER BY col.ordinal_position)),
			QUOTE_LITERAL(STRING_AGG(CONCAT('c.',k.column_name),',' ORDER BY col.ordinal_position)),
			QUOTE_LITERAL(STRING_AGG(CONCAT('f.',k.column_name),',' ORDER BY col.ordinal_position))
			INTO 
			_id_kolom, 
			_id_kolom_d1,
			_id_kolom_c,
			_id_kolom_f
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
	
	EXECUTE FORMAT ('SET LOCAL _%s._id_kolom = %s',_xid,_id_kolom);		
	EXECUTE FORMAT ('SET LOCAL _%s._id_kolom_d1 = %s',_xid,_id_kolom_d1);		
	EXECUTE FORMAT ('SET LOCAL _%s._id_kolom_c = %s',_xid,_id_kolom_c);		
	EXECUTE FORMAT ('SET LOCAL _%s._id_kolom_f = %s',_xid,_id_kolom_f);		

	SELECT 	QUOTE_LITERAL(STRING_AGG(QUOTE_IDENT(attname), ', ')), 
			QUOTE_LITERAL(STRING_AGG('($1).' || QUOTE_IDENT(attname), ', ')),
			QUOTE_LITERAL(STRING_AGG('f.' || QUOTE_IDENT(attname), ', '))
			INTO 	_kolommen, 
					_kolommen_new,
					_kolommen_futr
	FROM   pg_catalog.pg_attribute
	WHERE  	attrelid = CONCAT(TG_TABLE_SCHEMA,'.',TG_TABLE_NAME)::regclass AND 
			attisdropped = FALSE AND
			attnum > 0 AND
			NOT(LOWER(attname) = ANY(STRING_TO_ARRAY(_id_kolom,',')))
	;
	EXECUTE FORMAT ('SET LOCAL _%s._kolommen = %s',_xid,_kolommen);		
	EXECUTE FORMAT ('SET LOCAL _%s._kolommen_new = %s',_xid,_kolommen_new);		
	EXECUTE FORMAT ('SET LOCAL _%s._kolommen_futr = %s',_xid,_kolommen_futr);		
	
	-- En de rest van de triggers gewoon hun werk laten doen
	RETURN NEW; 
END;
$fnc_body$
LANGUAGE plpgsql
-- SECURITY DEFINER
SET search_path = generic, wh, pg_temp
;

ALTER FUNCTION generic.prep_auto_distribute OWNER TO dwh_owner;
GRANT EXECUTE ON FUNCTION generic.prep_auto_distribute TO dwh_owner, web_app_owners, web_app_default_users, pd_admin_users;
;

 /* ***************************************************************************************************** */
CREATE OR REPLACE FUNCTION generic.auto_distribute()
RETURNS TRIGGER AS 
$fnc_body$
DECLARE
	-- Het huidige transactie-ID
	_xid CHARACTER VARYING := (SELECT CONCAT('_',txid_current(),'.'));
	
     -- INSERT-waarden:
    _ID CHARACTER VARYING; 
    _input_geldig_vanaf DATE;	
	
	_id_kolom CHARACTER VARYING; -- De naam van de kolom die de sleutel bevat voor het betreffende record
	_id_kolom_d1 CHARACTER VARYING; -- _id_kolom(men), elk element voorafgegaan door '$1.'
	_id_kolom_c CHARACTER VARYING; -- _id_kolom(men), elk element voorafgegaan door 'c.'
	_id_kolom_f CHARACTER VARYING; -- _id_kolom(men), elk element voorafgegaan door 'f.' 
    _kolommen CHARACTER VARYING;
    _kolommen_new CHARACTER VARYING;
    _kolommen_futr CHARACTER VARYING;
	
	-- Welke datum is als actueel geregistreerd
    _current_geldig_vanaf DATE;    
	_bestaat BOOLEAN;
	
     -- Voor opbouw UPDATE-query
	_insert_tabel TEXT;
	
	
	_resultaten RECORD;
	-- _test TEXT;
	
	_id_found INTEGER;
	_max_id INTEGER;
BEGIN
	SELECT 	current_setting(CONCAT(_xid,'_id_kolom')),
			current_setting(CONCAT(_xid,'_id_kolom_d1')),
			current_setting(CONCAT(_xid,'_id_kolom_c')),
			current_setting(CONCAT(_xid,'_id_kolom_f')),
			current_setting(CONCAT(_xid,'_kolommen')),
			current_setting(CONCAT(_xid,'_kolommen_new')),
			current_setting(CONCAT(_xid,'_kolommen_futr'))
	INTO  	_id_kolom,
			_id_kolom_d1,
			_id_kolom_c,
			_id_kolom_f,
			_kolommen,
			_kolommen_new,
			_kolommen_futr
	;
	
	IF (NEW.geldig_vanaf IS NULL) THEN NEW.geldig_vanaf := CURRENT_DATE;
	END IF;
	_input_geldig_vanaf := NEW.geldig_vanaf;

	-- Voor het testen van 'null' in een ID-kolom:
	EXECUTE FORMAT ('SELECT COUNT(*)::INT aant_null FROM UNNEST(ARRAY[%s]::INTEGER[]) AS i WHERE i IS NULL',_id_kolom_d1) USING NEW INTO _id_found;
-- 	RAISE NOTICE '_id_found: (%)', _id_found;

	IF _id_found > 0 THEN
-- 		RAISE NOTICE 'Het id (%) bevat een NULL-waarde', _ID;

		-- Gewoon toevoegen
		--EXECUTE FORMAT('INSERT INTO %I.%I (%s) VALUES (%s)',TG_TABLE_SCHEMA,TG_TABLE_NAME,_kolommen,_kolommen_new) USING NEW;
		RETURN NEW;
	ELSE 
		-- En de betreffende waarde van de '_id'-kolom bepalen
		EXECUTE FORMAT('SELECT CONCAT_WS('','',%s)',_id_kolom_d1) USING NEW INTO _ID;
		--RAISE NOTICE '(%)=(%)',_id_kolom_d1, _ID;

		-- Kijken of deze uberhaupt bestaat
		EXECUTE FORMAT('SELECT geldig_vanaf FROM ONLY %I.%I WHERE (%s) = (%s)',TG_TABLE_SCHEMA,TG_TABLE_NAME, _id_kolom, _ID) INTO _current_geldig_vanaf;

		IF _current_geldig_vanaf IS NULL THEN 
			-- Nee, bestaat nog niet - simpelweg toevoegen
			RETURN NEW;
		ELSE 
			-- ------------------------------------------------------------------------------------------------
			-- Ja, is al wel bekend
			IF _current_geldig_vanaf::DATE > _input_geldig_vanaf::DATE 
			THEN	_insert_tabel = CONCAT(TG_TABLE_NAME,'_hist'); -- Dit is een (update/insert) op de HISTORISCHE tabel
			ELSEIF _current_geldig_vanaf::DATE = _input_geldig_vanaf::DATE THEN 
					_insert_tabel = TG_TABLE_NAME; -- Feitelijk een update op de huidige situatie
			ELSE	_insert_tabel = CONCAT(TG_TABLE_NAME,'_futr'); -- Dit is een (update/insert) op de TOEKOMST tabel
			END IF;

			-- RAISE NOTICE '_insert_tabel: %', _insert_tabel;
			_bestaat = FALSE;
			
			-- Bezien of er al een record in de juiste tabel bestaat met de opgegeven datum
			EXECUTE FORMAT('SELECT TRUE FROM %I.%I WHERE (%s) = (%s) AND geldig_vanaf = %L', TG_TABLE_SCHEMA, _insert_tabel, _id_kolom, _ID, _input_geldig_vanaf) INTO _bestaat;

			-- RAISE NOTICE '_bestaat: %', _bestaat;
			IF _bestaat = TRUE THEN 
				-- Jawel, bestaat al, dus een UPDATE
				EXECUTE FORMAT('UPDATE %I.%I SET (%s) = (%s) WHERE (%s) = (%s) AND geldig_vanaf = %L', TG_TABLE_SCHEMA, _insert_tabel, _kolommen, _kolommen_new, _id_kolom, _ID, _input_geldig_vanaf) USING NEW;
			ELSE 
				-- Nog niet, dus gewoon toevoegen
				EXECUTE FORMAT('INSERT INTO %I.%I VALUES ($1.*)',TG_TABLE_SCHEMA,_insert_tabel) USING NEW;
			END IF;
			-- ------------------------------------------------------------------------------------------------
		END IF;
		RETURN NULL; -- Rest, van wat er zou gebeuren, niet meer doen...
	END IF; -- Er is ID met een NULL-waarde	
END;
$fnc_body$
LANGUAGE plpgsql
--SECURITY DEFINER
SET search_path = generic, wh, pg_temp
;

ALTER FUNCTION generic.auto_distribute OWNER TO dwh_owner;
GRANT EXECUTE ON FUNCTION generic.auto_distribute TO dwh_owner, web_app_owners, web_app_default_users, pd_admin_users;
;
 
 /* ***************************************************************************************************** */

CREATE OR REPLACE FUNCTION generic.post_auto_distribute() 
RETURNS TRIGGER AS 
$fnc_body$
DECLARE
	-- Het huidige transactie-ID
	_xid CHARACTER VARYING := (SELECT CONCAT('_',txid_current(),'.'));
	
	_id_kolom CHARACTER VARYING; -- De naam van de kolom(men) die de sleutel bevat voor het betreffende record
	_id_kolom_c CHARACTER VARYING; -- _id_kolom(men), elk element voorafgegaan door 'c.'
	_id_kolom_f CHARACTER VARYING; -- _id_kolom(men), elk element voorafgegaan door 'f.' 
    
	_kolommen CHARACTER VARYING;
	_kolommen_futr CHARACTER VARYING;
	
	_resultaten RECORD;
	
BEGIN
	--RAISE NOTICE 'Uitvoeren post_auto_distribute op %.%', TG_TABLE_SCHEMA, TG_TABLE_NAME;
	SELECT 	current_setting(CONCAT(_xid,'_id_kolom')),
			current_setting(CONCAT(_xid,'_id_kolom_c')),
			current_setting(CONCAT(_xid,'_id_kolom_f')),
			current_setting(CONCAT(_xid,'_kolommen')),
			current_setting(CONCAT(_xid,'_kolommen_futr'))
	INTO  	_id_kolom,
			_id_kolom_c,
			_id_kolom_f,
			_kolommen,
			_kolommen_futr
	;

	-- Nu de toekomsttabel doorspitten of dat er datums zijn die vóór (of gelijk aan) vandaag zijn...
	-- Zoja, dan zoveel mogelijk vanuit de toekomst-tabel naar de huidige / historie tabel verplaatsen...
	FOR _resultaten IN 														  
		EXECUTE FORMAT ('	SELECT CONCAT_WS('','',%s) AS _id, geldig_vanaf
							FROM %I.%I_futr
							WHERE geldig_vanaf <= CURRENT_DATE
							ORDER BY geldig_vanaf ASC', _id_kolom, TG_TABLE_SCHEMA, TG_TABLE_NAME)
	LOOP
		-- Voor elk legitiem toekomst record dat naar de huidige tabel moet worden verplaatst: 
		EXECUTE FORMAT ('	INSERT INTO %I.%I_hist
							SELECT * 
							FROM ONLY %I.%I
							WHERE (%s) = (%s)', 
							TG_TABLE_SCHEMA, TG_TABLE_NAME, 
							TG_TABLE_SCHEMA, TG_TABLE_NAME, 
							_id_kolom, _resultaten._id);

		-- ... en de update op de 'huidige' tabel uitvoeren 
		EXECUTE FORMAT('UPDATE ONLY %I.%I AS c 
						SET (%s) = (%s) 
						FROM (	SELECT * 
								FROM %I.%I_futr 
								WHERE 	(%s) = (%s) AND 
										geldig_vanaf = %L) AS f
						WHERE 	(%s) = (%s) AND
								(%s) = (%s)', 
						TG_TABLE_SCHEMA, TG_TABLE_NAME, 
						_kolommen, _kolommen_futr, 
						TG_TABLE_SCHEMA, TG_TABLE_NAME, 
						_id_kolom, _resultaten._id,
						_resultaten.geldig_vanaf,
						_id_kolom_c, _id_kolom_f,
						_id_kolom_c, _resultaten._id);

		--En de delete in de FUTR-tabel
		EXECUTE FORMAT ('	DELETE 
							FROM %I.%I_futr
							WHERE 	geldig_vanaf = %L AND
									(%s) = (%s)', 
							TG_TABLE_SCHEMA, TG_TABLE_NAME, 
							_resultaten.geldig_vanaf, 
							_id_kolom, _resultaten._id);

	END LOOP;
	
RETURN NULL;
END;
$fnc_body$
LANGUAGE plpgsql
SET search_path = 'generic, wh, pg_temp'
;

ALTER FUNCTION generic.post_auto_distribute OWNER TO dwh_owner;
GRANT EXECUTE ON FUNCTION generic.post_auto_distribute TO dwh_owner, web_app_owners, pd_admin_users;
