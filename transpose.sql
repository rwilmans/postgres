CREATE OR REPLACE FUNCTION generic.transpose(
	ds_in text DEFAULT ''::text,
	ds_out text DEFAULT 'transpose_out'::text,
	by_vars text DEFAULT ''::text,
	category_var text DEFAULT ''::text,
	prefix text DEFAULT '_'::text)
    RETURNS boolean
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$
DECLARE
/* 
FUNCTION TRANSPOSE

PARAMETERS:
-----------
ds_in 		:	(qualified) table name that serves as input. 
				No Default - REQUIRED
ds_out 		:	name of the (temporary!) table that will be created containing the output results. 
				DEFAULT 'transpose_out'
by_vars 	:	the column (or set of columns, separated by a comma) that serves as an identifier by which all the value-columns are transposed
				So every unique set of by_vars will result in ONE output-row (but with different columns, depending on 'category_var').
				No default.
				If no by_vars are given, the IDENTITY columns will be used. If there are none, the FIRST column will be used (first, according to PG)
category_var:	column that contains the 'labels' of the transposed values; the columns in the output will be named conform the values of this column
				If more than one (column name is) given, only the FIRST will be used. 
				No default. When empty (/not given) then the prefix will be used.
prefix 		:	the prefix that will be used if no category-var is given of found. The resulting (transposed) columns will be named '<prefix><rownumber>'
				with rownumber being the n-th row within the by_vars-group. 
				Default '_'. Not used when (valid) category_var given. 
				
RETURNS: 	
--------
BOOLEAN (TRUE/FALSE). TRUE; succes. FALSE; an error has occurred.

Example:
-- ---------------------------------------------------------------
DROP TABLE IF EXISTS raoul;
CREATE TEMPORARY TABLE raoul (rijnr INTEGER, categorie CHARACTER VARYING(5), waarde INTEGER, leeftijd INTEGER);
INSERT INTO raoul (rijnr, categorie, waarde, leeftijd)
VALUES 	(1,'man1', 4,18),
		(1,'man2', 3,44),
		(1,'man3', 2,21),
		(1,'man4', 1,35),
		(2,'man2', 22,29),
		(2,'man3', 23,17);
-- ---------------------------------------------------------------
SELECT administratie.transpose(	ds_in => 'raoul', 
							   	ds_out => 'r_xpose', 
							   	by_vars => 'rijnr', 
							   	category_var => 'categorie');
SELECT * FROM r_xpose;
-- ---------------------------------------------------------------
SELECT administratie.transpose(	ds_in => 'raoul', 
								category_var => 'categorie');
SELECT * FROM transpose_out;
-- ---------------------------------------------------------------
SELECT administratie.transpose(	ds_in => 'raoul', 
								by_vars => 'rijnr, categorie');
SELECT * FROM transpose_out;
-- ---------------------------------------------------------------

*/

_tabel TEXT;
_schema TEXT;
_by_vars TEXT;
_b_by_vars TEXT;
_kolommen TEXT;
_waarde_kolom TEXT;
_iteraties INTEGER;
_categorieen TEXT;
_categorieen_vars TEXT;
_temp_table TEXT;
_del_tmptbl TEXT[];
_query TEXT;
_tf_schema TEXT;

BEGIN

PERFORM 1
FROM pg_available_extensions
WHERE 	installed_version IS NOT NULL AND
		name = 'tablefunc';		
IF NOT FOUND THEN
	EXECUTE 'CREATE EXTENSION tablefunc';
END IF;

IF ds_in != '' AND ds_in IS NOT NULL THEN 

	-- Puinbakken / TEMP-tables (__temp_&ds_out) weggooien
	EXECUTE	FORMAT('SELECT ARRAY_AGG(COALESCE(table_name,NULL))::TEXT AS tmptbl, COUNT(*) AS iteraties FROM (SELECT DISTINCT table_name FROM information_schema.columns WHERE table_name ~ ''^(__temp_)?%s'') AS b' ,ds_out,ds_out) INTO _del_tmptbl, _iteraties;
	
	FOR i IN 1.._iteraties LOOP
		EXECUTE FORMAT('DROP TABLE %I;', _del_tmptbl[i]);
	END LOOP;
	
	-- ds_in controleren
	IF (POSITION('.' IN ds_in) >0) THEN
		_tabel = SPLIT_PART(ds_in,'.',2);
		_schema = SPLIT_PART(ds_in,'.',1);
	ELSE 
		SELECT 	c.relname, n.nspname 
				INTO 
				_tabel, _schema
		FROM	pg_catalog.pg_class AS c,
				pg_catalog.pg_namespace AS n,
				(	SELECT 	pad,
							ROW_NUMBER() OVER() AS volgorde
					FROM (SELECT UNNEST(CURRENT_SCHEMAS(TRUE)) AS pad) AS b
					) AS p
		WHERE 	n.oid = c.relnamespace AND
				c.relname = ds_in AND    
				c.relkind = 'r' AND
				n.nspname = p.pad
		ORDER BY p.volgorde
		LIMIT 1;
	END IF;
						 
	-- by_vars controleren en in _by_vars zetten...
 	IF by_vars = '' THEN 
	 	-- Als er identity-kolommen zijn, dan deze gebruiken,
		-- anders de (volgens PG) 'eerste' kolom 
		WITH basis AS (	SELECT column_name, is_identity, ordinal_position
						FROM INFORMATION_SCHEMA.COLUMNS
						WHERE table_name = _tabel)
		SELECT 	STRING_AGG(column_name, ', ' ORDER BY ordinal_position) AS by_vars_new,
				STRING_AGG(CONCAT('t.',column_name), ', ' ORDER BY ordinal_position) AS b_by_vars
				INTO _by_vars, _b_by_vars
		FROM 	basis,
				(	SELECT CASE WHEN COUNT(*) > 0 THEN TRUE ELSE FALSE END AS has_identity
					FROM basis
					WHERE is_identity = 'YES') AS ii
		WHERE 	CASE WHEN ii.has_identity THEN is_identity = 'YES'
						ELSE ordinal_position = 1
				END
		;
	ELSE
		SELECT 	STRING_AGG(k.kolom,', ' ORDER BY k.volgorde) AS by_vars_new,
				STRING_AGG(CONCAT('t.',k.kolom),', ' ORDER BY k.volgorde) AS b_by_vars
				INTO _by_vars, _b_by_vars
		FROM (	SELECT 	k.kolom, 
						ROW_NUMBER() OVER () AS volgorde
				FROM (SELECT REGEXP_SPLIT_TO_TABLE(by_vars, ',\s*') AS kolom) AS k
				) AS k,
			 INFORMATION_SCHEMA.COLUMNS AS c
		WHERE 	k.kolom = c.column_name AND
				c.table_name = _tabel
		;
-- RAISE NOTICE '_by_vars: %  ... _b_by_vars: %', _by_vars, _b_by_vars;
	END IF;

	-- Controle op categorie-var;
	IF category_var != '' THEN 
		IF (SELECT ARRAY_LENGTH(REGEXP_SPLIT_TO_ARRAY(category_var, ',\s*'),1) > 1 ) THEN 
			SELECT 	arr[1] 
					INTO 
					category_var
			FROM (SELECT REGEXP_SPLIT_TO_ARRAY(category_var, ',\s*') AS arr) AS a ;
		END IF;
	END IF;
	
-- RAISE NOTICE 'category_var: %', category_var;
	
	SELECT CONCAT('__temp_',ds_out) INTO _temp_table;
	
	BEGIN	
		-- Controle op prefix
		-- prefix = REGEXP_REPLACE(REGEXP_REPLACE(CASE WHEN prefix ~ '^\d' THEN CONCAT('_',prefix) ELSE prefix END,'[^a-zA-Z0-9]', '_','g'),'(_)+','_','g');
	
		-- Nu de pseudo-rijnr bepalen (een volgnummer per set aan unieke by_vars)
		IF category_var = '' THEN 
			-- Dan maken we zelf een categorie-variabele aan, obv het rijnummer binnen de __groepnr
			_query := FORMAT('CREATE TEMPORARY TABLE %I AS SELECT DENSE_RANK() OVER (ORDER BY %s) AS __groepnr , CONCAT(''%s'',ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s)) AS __cat_var, * FROM %I.%I;', 
						   	_temp_table, _by_vars, prefix, _by_vars, _by_vars, _schema, _tabel ) ;
			EXECUTE _query;
		ELSE
			_query := FORMAT('CREATE TEMPORARY TABLE %I AS SELECT DENSE_RANK() OVER (ORDER BY %s) AS __groepnr , * FROM %I.%I;', _temp_table, _by_vars, _schema, _tabel ) ;
			EXECUTE _query;
-- RAISE NOTICE 'klaar TEMP-table %',_query;
-- SELECT STRING_AGG(relname,', ') INTO _query FROM pg_catalog.pg_class WHERE relname ~ '^_' AND relkind = 'r';
-- RAISE NOTICE 'gevonden temp-tabellen: %', _query;
		END IF;

		-- Nu het aantal te transponeren kolommen bepalen en tevens het # iteraties dat daarvoor nodig is
		SELECT 	STRING_AGG(column_name,',') AS kolommen, 
				COUNT(*) AS aantal 
				INTO _kolommen, _iteraties
		FROM INFORMATION_SCHEMA.COLUMNS AS c
		LEFT JOIN 
			(SELECT UNNEST(ARRAY_APPEND(REGEXP_SPLIT_TO_ARRAY(_by_vars, ',\s*'),category_var)) AS kolom) AS k
		ON	c.column_name = k.kolom
		WHERE 	k.kolom IS NULL AND
				c.TABLE_NAME = _tabel
		;			

-- RAISE NOTICE ' _kolommen: %  ...  _iteraties: %', _kolommen, _iteraties;
		SELECT 	n.nspname INTO _tf_schema
		FROM 	pg_catalog.pg_extension AS e,
				pg_catalog.pg_namespace AS n
		WHERE 	n.oid = e.extnamespace AND
				e.extname = 'tablefunc'
		;

		IF category_var = '' THEN 
			-- Er is GEEN categorie-var benoemd, 
			-- dus alle overige kolommen transponeren 

			FOR i IN 1.._iteraties LOOP

				SELECT SPLIT_PART(_kolommen,',',i) INTO _waarde_kolom;

-- RAISE NOTICE 'Iteratie: (%/%), _waarde_kolom: %',i,_iteraties,_waarde_kolom;

				-- Eerst de 'categorie-waarden'(die we nu gelijk nemen aan de DISTINCT VALUES van de BY_VARs) en het datatype van de waarde-kolom achter elkaar zetten
				EXECUTE FORMAT('SELECT 	STRING_AGG(CONCAT(	kol, 
															'' '',
							   								(	SELECT CASE WHEN data_type = ''integer'' THEN data_type ELSE CONCAT(data_type, ''('', CHARACTER_MAXIMUM_LENGTH,'')'') END AS datatype 
																FROM INFORMATION_SCHEMA.COLUMNS 
																WHERE TABLE_SCHEMA = ''%I'' AND TABLE_NAME = ''%I'' AND COLUMN_NAME = ''%I''
							  								)
														), '', '' ),
							   			STRING_AGG(kol, '', '' ) 
							   	FROM (	SELECT CONCAT(''%s'',GENERATE_SERIES(1,(SELECT MAX(aant)
							   													FROM (SELECT COUNT(*) AS aant FROM %I GROUP BY __groepnr) AS i
							  													)
																			)
							  							) AS kol
							  			) AS b;', 
							   _schema, _tabel, _waarde_kolom, prefix, _temp_table) 
							   INTO _categorieen, _categorieen_vars;

				-- De feitelijke CROSSTAB-tabel maken
				EXECUTE FORMAT('CREATE TEMPORARY TABLE %s AS SELECT * FROM %s.CROSSTAB(''SELECT __groepnr AS row_name, %s AS _original_category, __cat_var AS category, %s AS value FROM %s ORDER BY __groepnr'', '
							   ' ''SELECT CONCAT(%s,GENERATE_SERIES(1,(SELECT MAX(aant) FROM (SELECT COUNT(*) AS aant FROM %I GROUP BY __groepnr) AS i)))'' ) AS (__groepnr INTEGER, _original_category TEXT, %s) ;',  
							   	CONCAT(_temp_table,'_', i), _tf_schema, CONCAT('''''',_waarde_kolom,''''''), _waarde_kolom, _temp_table, 
								CONCAT('''''',prefix,''''''), _temp_table,_categorieen) ;
			END LOOP;

		ELSE
			-- Er is dus WEL een categorie-kolom opgegeven			
			
			-- De opsomming van de categorien-variabelen
			EXECUTE FORMAT('SELECT STRING_AGG(e.kol,'', '') FROM (SELECT DISTINCT %I AS kol FROM %I ORDER BY 1) AS b,
									LATERAL (SELECT LOWER(REGEXP_REPLACE(REGEXP_REPLACE(CASE WHEN b.kol ~ ''^\d'' THEN CONCAT(''_'',b.kol) ELSE b.kol END,''[^a-zA-Z0-9]'', ''_'',''g''),''(_)+'',''_'',''g'')) AS kol) AS e', category_var, _temp_table) INTO _categorieen_vars;
		
			-- Itereren over de waarde-kolommen
			FOR i IN 1.._iteraties LOOP

				SELECT SPLIT_PART(_kolommen,',',i) INTO _waarde_kolom;

-- RAISE NOTICE 'Iteratie: (%/%), _waarde_kolom: %',i,_iteraties,_waarde_kolom;

				-- Eerst de categorie-waarden en het datatype van de waarde-kolom achter elkaar zetten
				EXECUTE FORMAT('SELECT STRING_AGG(CONCAT(	e.kol,
							   								'' '',
							   								(	SELECT CASE WHEN data_type = ''integer'' OR data_type = ''text'' THEN data_type ELSE CONCAT(data_type, ''('', CHARACTER_MAXIMUM_LENGTH,'')'') END AS datatype 
																FROM INFORMATION_SCHEMA.COLUMNS 
																WHERE TABLE_SCHEMA = ''%I'' AND TABLE_NAME = ''%I'' AND COLUMN_NAME = ''%I'')), '', '' ) 
							   	FROM (SELECT DISTINCT %I AS kol FROM %I ORDER BY 1) AS b,
									LATERAL (SELECT LOWER(REGEXP_REPLACE(REGEXP_REPLACE(CASE WHEN b.kol ~ ''^\d'' THEN CONCAT(''_'',b.kol) ELSE b.kol END,''[^a-zA-Z0-9]'', ''_'',''g''),''(_)+'',''_'',''g'')) AS kol) AS e;', 
							   _schema, _tabel, _waarde_kolom, category_var, _temp_table) 
							   INTO _categorieen;
-- RAISE NOTICE '_categorieen: %', _categorieen;
							   
				-- De feitelijke CROSSTAB-tabel maken
				_query := FORMAT ('CREATE TEMPORARY TABLE %s AS SELECT * FROM %s.CROSSTAB(''SELECT __groepnr AS row_name, %s AS _original_category, %s AS category, %s AS value FROM %s ORDER BY __groepnr'','
								   ' ''SELECT DISTINCT %s FROM %s ORDER BY 1'' ) AS (__groepnr INTEGER, _original_category TEXT, %s) ;',  
								   CONCAT(_temp_table,'_', i), _tf_schema,  CONCAT('''''',_waarde_kolom,''''''), category_var, _waarde_kolom, _temp_table, category_var, _temp_table,_categorieen) ;
-- RAISE NOTICE 'create: %',_query;						   
				EXECUTE _query;
-- EXECUTE FORMAT('SELECT COUNT(*) FROM %s',CONCAT(_temp_table,'_', i)) INTO _query;
-- RAISE NOTICE 'Gevonden rijen in %: %', CONCAT(_temp_table,'_', i),_query;

			END LOOP;
			
		END IF;

		-- Tabel __temp_xxxxx_bijna aanmaken voor het aan elkaar plakken van de gemaakte afzonderlijke tabellen...
		EXECUTE FORMAT('CREATE TEMPORARY TABLE %s_bijna AS SELECT * FROM %s_1 WITH NO DATA;', _temp_table, _temp_table) ;
		
		-- Aan elkaar knopen van de temp-tabellen
		FOR i IN 1.._iteraties LOOP
			EXECUTE FORMAT('INSERT INTO %s_bijna SELECT * FROM %s_%s', _temp_table, _temp_table, i) ;		
		END LOOP;

		-- Hier de uiteindelijke tabel aanmaken
		-- En de juiste by-var kolom(men) er weer tegenaan plakken
		_query = FORMAT('CREATE TEMPORARY TABLE %I AS SELECT %s, _original_category, %s FROM (SELECT DISTINCT ON (__groepnr) __groepnr, %s FROM %I) AS t, %s_bijna AS b WHERE t.__groepnr = b.__groepnr' ,
					   ds_out, _b_by_vars, _categorieen_vars, _by_vars, _temp_table, _temp_table );
-- RAISE NOTICE 'Voor TempTABLE by-var plakken: %',_query; 
		EXECUTE _query;
-- RAISE NOTICE 'Na TempTABLE by-var plakken';


		-- Puinbakken
		_query = FORMAT('SELECT ARRAY_AGG(COALESCE(table_name,NULL))::TEXT AS tmptbl, COUNT(*) AS iteraties FROM (SELECT DISTINCT table_name FROM information_schema.columns WHERE table_name ~ ''^__temp_%s'') AS b' ,ds_out,ds_out) ; 
-- RAISE NOTICE 'Opvragen temp-tabellen: %', _query;		
		EXECUTE	_query INTO _del_tmptbl, _iteraties;
		
		FOR i IN 1.._iteraties LOOP
			EXECUTE FORMAT('DROP TABLE IF EXISTS %I;', _del_tmptbl[i]);
		END LOOP;
		
	EXCEPTION WHEN OTHERS THEN 
		RAISE NOTICE 'Error opgetreden bij het transponeren';
		RETURN FALSE;	
	END;	
	RETURN TRUE;
	
END IF; -- ds_in gevuld
RETURN FALSE;

END;
$BODY$;

ALTER FUNCTION generic.transpose(text, text, text, text, text) OWNER TO dwh_owner;
