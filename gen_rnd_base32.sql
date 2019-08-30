-- DROP FUNCTION gen_rnd_base32;
CREATE OR REPLACE FUNCTION gen_rnd_base32(keylength INTEGER DEFAULT 20)
RETURNS TEXT AS
$BODY$
DECLARE
/*
FUNCTION GEN_RND_BASE32

AUTHOR:
-------
R. Wilmans-Sangwienwong (2019)

PURPOSE:
--------

To generate a random string of length [keylength] in base32. That is, only (uppercase) letters
will be used, as well as digits 2 - 7 (inclusive).
This may be used to provide a server key for (T)OTP purposes.

PARAMETERS:
-----------
keylength	:	Number of characters the output-string will contain.
				Datatype: INTEGER
				Default 20

RETURNS:
--------
TEXT / String of length [keylength].

Example:
-- ---------------------------------------------------------------
SELECT gen_rnd_base32(25);
-- ---------------------------------------------------------------
*/

_secretkey TEXT;
_b32arr TEXT[];
_basis BYTEA;
_aantal_bytes INTEGER;
_bitstring TEXT;
_b32_char TEXT;

BEGIN
	-- Bestaat de PGCRYPTO extensie? Zonee, installeren
	PERFORM 1
	FROM pg_available_extensions
	WHERE 	installed_version IS NOT NULL AND
			name = 'pgcrypto';
	IF NOT FOUND THEN
		EXECUTE 'CREATE EXTENSION pgcrypto';
	END IF;

	_b32arr = '{"A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z","2","3","4","5","6","7"}';

	_aantal_bytes = CEIL(keylength*5::NUMERIC/8)::INT; -- Te genereren aantal bytes
-- 	RAISE NOTICE '# bytes: % ', _aantal_bytes;
	_basis = GEN_RANDOM_BYTES(_aantal_bytes); -- Random bytes (#: _aantal_bytes)
-- 	RAISE NOTICE '_basis: % ', ENCODE(_basis,'HEX');

 	FOR i IN 1.._aantal_bytes LOOP
		-- Aan elkaar plakken
		_bitstring := CONCAT(_bitstring, GET_BYTE(_basis,i-1)::BIT(8)::CHAR(8)); -- Bitstring terlengte van (#: _aantal_bytes) bytes
	END LOOP;
-- 	RAISE NOTICE '%',_bitstring;

 	FOR i IN 1..keylength LOOP
		-- Ophakken in stukken van 5 bits
		_b32_char = SUBSTR(_bitstring,(i-1)*5+1,5); -- De i-de 5-bit string
-- 		RAISE NOTICE '%: % , int (%) als B32: %',i,stukje, _b32_char::BIT(5)::INT, _b32arr[_b32_char::BIT(5)::INT+1];
		_secretkey := CONCAT(_secretkey, _b32arr[_b32_char::BIT(5)::INT+1]);
	END LOOP;

	RETURN _secretkey;
END;
$BODY$
LANGUAGE plpgsql;
