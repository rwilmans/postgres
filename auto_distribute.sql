/* FNC_AUTO_DISTRIBUTE */
CREATE OR REPLACE FUNCTION generic.fnc_auto_distribute()
RETURNS TRIGGER AS
$fnc_body$
DECLARE
    -- INSERT-waarden:
   _ID CHARACTER VARYING;
   _input_geldig_vanaf DATE;
 _id_kolom CHARACTER VARYING; -- De naam van de kolom die de sleutel bevat voor het betreffende record
 _id_kolom_d1 CHARACTER VARYING; -- _id_kolom, elk element voorafgegaan door '$1.'
 _id_kolom_c CHARACTER VARYING; -- _id_kolom, elk element voorafgegaan door 'c.'
 _id_kolom_f CHARACTER VARYING; -- _id_kolom, elk element voorafgegaan door 'f.'

 -- Welke datum is als actueel geregistreerd
   _current_geldig_vanaf DATE;

 _bestaat BOOLEAN;

    -- Voor opbouw UPDATE-query
 _insert_tabel TEXT;
   _kolommen TEXT;
   _kolommen_new TEXT;
   _kolommen_futr TEXT;

 _resultaten RECORD;
 _test TEXT;
BEGIN

 IF (NEW.geldig_vanaf IS NULL) THEN NEW.geldig_vanaf := CURRENT_DATE;
 END IF;
 _input_geldig_vanaf := NEW.geldig_vanaf;

 -- Uitzoeken wat de '_id'-kolom is (dat is dus de primary key op de 'current'-tabel)
 EXECUTE FORMAT('SELECT 	STRING_AGG(k.column_name,'','' ORDER BY col.ordinal_position),
               STRING_AGG(CONCAT(''$1.'',k.column_name),'','' ORDER BY col.ordinal_position),
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
             ', TG_TABLE_NAME,TG_TABLE_SCHEMA)
             INTO
             _id_kolom,
             _id_kolom_d1,
             _id_kolom_c,
             _id_kolom_f
             ;

 -- En de betreffende waarde bepalen
 EXECUTE FORMAT('SELECT CONCAT_WS('','',%s)',_id_kolom_d1) USING NEW INTO _ID;

-- RAISE NOTICE 'Het id (%): %', _id_kolom, _ID;

 -- Kijken of deze uberhaupt bestaat

 EXECUTE FORMAT('SELECT geldig_vanaf FROM ONLY %I.%I WHERE (%s) = (%s)',TG_TABLE_SCHEMA,TG_TABLE_NAME, _id_kolom, _ID) INTO _current_geldig_vanaf;

 IF _current_geldig_vanaf IS NULL THEN
   -- Nee, bestaat nog niet - simpelweg toevoegen
   RETURN NEW;
 ELSE
   -- Ja, is al wel bekend

   -- ------------------------------------------------------------------------------------------------
   -- De kolommen achter elkaar zetten
   SELECT 	STRING_AGG(QUOTE_IDENT(attname), ', '),
       STRING_AGG('($1).' || QUOTE_IDENT(attname), ', ') ,
       STRING_AGG('f.' || QUOTE_IDENT(attname), ', ')
       INTO 	_kolommen,
           _kolommen_new,
           _kolommen_futr
   FROM   pg_catalog.pg_attribute
   WHERE  	attrelid = CONCAT(TG_TABLE_SCHEMA,'.',TG_TABLE_NAME)::regclass AND
       attisdropped = FALSE AND
       attnum > 0 AND
       NOT(LOWER(attname) = ANY(STRING_TO_ARRAY(_id_kolom,',')))
   ;

-- RAISE NOTICE '_kolommen: %', _kolommen_new;

   IF _current_geldig_vanaf::DATE > _input_geldig_vanaf::DATE
   THEN	_insert_tabel = CONCAT(TG_TABLE_NAME,'_hist'); -- Dit is een (update/insert) op de HISTORISCHE tabel
   ELSEIF _current_geldig_vanaf::DATE = _input_geldig_vanaf::DATE THEN
       _insert_tabel = TG_TABLE_NAME; -- Feitelijk een update op de huidige situatie
   ELSE	_insert_tabel = CONCAT(TG_TABLE_NAME,'_futr'); -- Dit is een (update/insert) op de TOEKOMST tabel
   END IF;

-- RAISE NOTICE '_insert_tabel: %', _insert_tabel;
   -- Bezien of er al een record in de juiste tabel bestaat met de opgegeven datum
   EXECUTE FORMAT('SELECT TRUE FROM %I.%I WHERE (%s) = (%s) AND geldig_vanaf = %L', TG_TABLE_SCHEMA, _insert_tabel, _id_kolom, _ID, _input_geldig_vanaf) INTO _bestaat;

-- RAISE NOTICE '_bestaat: %', _bestaat;

   IF _bestaat IS NULL THEN
     -- Nog niet, dus gewoon toevoegen
     EXECUTE FORMAT('INSERT INTO %s VALUES ($1.*)',_insert_tabel) USING NEW;
   ELSE
     -- Jawel, bestaat al, dus een UPDATE
     EXECUTE FORMAT('UPDATE %I.%I SET (%s) = (%s) WHERE (%s) = (%s) AND geldig_vanaf = %L', TG_TABLE_SCHEMA, _insert_tabel, _kolommen, _kolommen_new, _id_kolom, _ID, _input_geldig_vanaf) USING NEW;
   END IF;

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

-- RAISE NOTICE 'Auto-move % (%) / %', _id_kolom, _ID, _resultaten.geldig_vanaf;
   END LOOP;

   -- ------------------------------------------------------------------------------------------------
 END IF;
 RETURN NULL; -- Rest, van wat er zou gebeuren, niet meer doen...
END;
$fnc_body$
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = generic, pg_temp
;
