CREATE OR REPLACE FUNCTION get_otp(timeframe INT DEFAULT 30, totp_digits INT DEFAULT 6, baa INT DEFAULT 0, test_key TEXT DEFAULT '')
RETURNS TEXT[] AS
$BODY$
DECLARE
/*
FUNCTION GET_OTP

AUTHOR:
-------
R. Wilmans-Sangwienwong (2019)

PURPOSE:
--------
Generate (a set of) TOTP, based on the SESSION_USER and the existence of that user in the table 'administratie.login.login' (easily changed to whatever) and
the corresponding secret key ('login.secret_key').
This can be used for either requesting an OTP or used for checking if a provided OTP matche.

PARAMETERS:
-----------
timeframe	: 	The time (in full seconds) before the OTP expires.
				Datatype: INTEGER
				Default: 30 (seconds)
totp_digits	: 	The nunmber of digits to be used as a TOTP
				Datatype: INTEGER
				Default: 6
baa			:	Before-And-After: extra timeframes calculated in order to account for differences in system-times
				baa => 2 means two timeframes BEFORE current, the current timeframe and two timeframes AFTER the current timeframe
				Datatype: INTEGER
				Default: 0 (so only the current timeframe)
test_key	:	The 'secret-key' to use, instead of using the actual sercret_key (as stored in table 'adminstratie.login')

RETURNS:
--------
TEXT[] / Array with strings, with every string representing the TOTP for the requested timeframes.

REMARKS:
--------
xxxxx

Example:
-- ---------------------------------------------------------------
SELECT get_otp(timeframe => 30, baa=>3);
-- ---------------------------------------------------------------
SELECT get_otp(timeframe => 30, baa=>1, test_key => 'JBSWY3DPEHPK3PXP' ); // Works well to test with http://blog.tinisles.com/2011/10/google-authenticator-one-time-password-algorithm-in-javascript/
-- --------------------------------------------------------------
SELECT  '123456' = ANY(get_otp())
-- --------------------------------------------------------------
*/

_totp TEXT[];
_crypto_function TEXT;

BEGIN
_crypto_function = 'sha1';

-- Voor elk van de benodigde timeframes de tijd-parameter bepalen
SELECT 	 ARRAY_AGG(RIGHT((CONCAT('x',th.truncted_hash)::BIT(32) & 'x7fffffff')::BIGINT::TEXT, totp_digits)) AS totp INTO _totp
FROM (	SELECT 	ENCODE(HMAC(tijdparm,secret_key,_crypto_function), 'HEX') AS hash
		FROM (	SELECT 	DECODE(LPAD(TO_HEX(ROUND(EXTRACT(EPOCH FROM (TRANSACTION_TIMESTAMP() + _offset*INTERVAL '1 SEC'))/timeframe)::INT),16,'0'),'HEX') AS tijdparm
				FROM (SELECT timeframe*GENERATE_SERIES(-baa,baa) AS _offset) AS tf
				) AS t,
			(	SELECT 	CASE WHEN (test_key != '') THEN base32_decode(test_key)
			 					ELSE base32_decode(secret_key)
			 			END AS secret_key
				FROM administratie.login
				WHERE 	CASE WHEN (test_key != '') THEN TRUE
			 				ELSE login = SESSION_USER /* In eigenlijke functie : SESSION_USER */
			 			END
			 	LIMIT 1
				) AS sk
		) AS h,
	LATERAL (SELECT GET_BYTE(DECODE(CONCAT('0',RIGHT(h.hash,1)),'HEX'),0) AS _offset) AS l,
	LATERAL (SELECT SUBSTR(h.hash,1+l._offset*2,4*2) AS truncted_hash) AS th
;

RETURN _totp;
END;
$BODY$
LANGUAGE plpgsql;
