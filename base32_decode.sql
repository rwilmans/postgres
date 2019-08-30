-- DROP FUNCTION IF EXISTS base32_decode;
CREATE OR REPLACE FUNCTION base32_decode(string TEXT DEFAULT '')
RETURNS BYTEA AS
$BODY$
DECLARE
/*
FUNCTION BASE32_DECODE

AUTHOR:
-------
R. Wilmans-Sangwienwong (2019)

PURPOSE:
--------

To decode the given string (base32-coded!) and return the bytes AS BYTEA.
This will, conform the useage of Google, return only the number of bytes that are fully defined
by the input base32 string. Any residual bits (less than 8, thus not 'filling up' to a whole byte)
will NOT be returned (and thus 'discarded').
Future development may see an option to return the 'not fully filled byte' (padded with zeros for the residual bits).

PARAMETERS:
-----------
string	:	A base-32 encoded string
			Datatype: TEXT
			No default

RETURNS:
--------
BYTEA / Bytestring representing all fully-defined bytes of the input-string.

REMARKS:
--------
Any non-base32 characters will be removed prior to decoding the base32 input string.

Example:
-- ---------------------------------------------------------------
SELECT base32_decode('ABSWY3DPEHPK3PXA');
-- ---------------------------------------------------------------
*/

_b32arr TEXT[];

_aantal_bytes INTEGER;
_bitstring TEXT;
_eenbyte TEXT;
_eenbyte_hex TEXT;
_hex_string TEXT;

BEGIN
IF (string != '') THEN
	-- Opschonen
	string := REGEXP_REPLACE(UPPER(string),'[^A-Z2-7]*','','g');

	_b32arr = ARRAY['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z','2','3','4','5','6','7'];

 	FOR i IN 1..LENGTH(string) LOOP
		-- Aan elkaar plakken
		_bitstring = CONCAT(_bitstring, ((ARRAY_POSITION(_b32arr, SUBSTR(string,i,1)) - 1)::BIT(5))::CHAR(5));
	END LOOP;
	-- RAISE NOTICE 'Bitstring: %', _bitstring;

	-- FLOOR(LENGTH(string)*5/8) is the number of bytes
 	FOR i IN 1..(FLOOR(LENGTH(string)*5/8)) LOOP
		_eenbyte = SUBSTR(_bitstring,(i-1)*8+1,8); -- Per byte
		_eenbyte_hex = LPAD(TO_HEX(_eenbyte::BIT(8)::INT),2,'0'); -- In hex-vorm (voorloopnul!)
		_hex_string = CONCAT(_hex_string,_eenbyte_hex); -- Aan elkaar plakken
	END LOOP;

	RETURN DECODE(_hex_string,'HEX');
ELSE
	-- Niets te doen....
	RETURN NULL;
END IF;
END;
$BODY$
LANGUAGE plpgsql;
