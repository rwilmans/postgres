CREATE OR REPLACE FUNCTION administratie.verify_otp(for_login TEXT DEFAULT '', given_totp TEXT DEFAULT '', timeframe INT DEFAULT 30, totp_digits INT DEFAULT 6, baa INT DEFAULT 1)
RETURNS BOOLEAN AS
$BODY$
DECLARE
/*
FUNCTION VERIFY_OTP

PURPOSE:
--------
Check given TOTP, based on the given login.

PARAMETERS:
-----------
for_login	:	The login as registered in table administratie.login ('login'), to use the corresponding secret key ('secret_key'),
				to calculate TOTP.
				Datatype: TEXT
				Default: ''
given_totp	:	The TOTP to verify (given as a text)
				Datatype: TEXT
				Default: ''
timeframe	: 	The time (in full seconds) before the OTP expires.
				Datatype: INTEGER
				Default: 30 (seconds)
totp_digits	: 	The nunmber of digits to be used as a TOTP
				Datatype: INTEGER
				Default: 6
baa			:	Before-And-After: extra timeframes calculated in order to account for differences in system-times
				baa => 2 means two timeframes BEFORE current, the current timeframe and two timeframes AFTER the current timeframe
				Datatype: INTEGER
				Default: 1 (so one timeframe before the current timeframe, the current timeframe and one timeframe after the current timeframe)

RETURNS:
--------
BOOLEAN: TRUE if TOTP is verified, FALSE if falsified.

REMARKS:
--------
xxxxx

Example:
-- ---------------------------------------------------------------
SELECT verify_otp(for_login => '_84f28fe026190c9b885dc74b63759893', given_totp => '404112')
-- ---------------------------------------------------------------
*/

_totp TEXT[];

BEGIN

-- Voor elk van de benodigde timeframes de tijd-parameter bepalen
SELECT 	 ARRAY_AGG(RIGHT((CONCAT('x',th.truncted_hash)::BIT(32) & 'x7fffffff')::BIGINT::TEXT, totp_digits)) AS totp INTO _totp
FROM (	SELECT 	ENCODE(HMAC(tijdparm,secret_key,'sha1'), 'HEX') AS hash
		FROM (	SELECT 	DECODE(LPAD(TO_HEX(ROUND(EXTRACT(EPOCH FROM (TRANSACTION_TIMESTAMP() + _time_offset*INTERVAL '1 SEC'))/timeframe)::INT),16,'0'),'HEX') AS tijdparm
				FROM (SELECT timeframe*GENERATE_SERIES(-baa,baa) AS _time_offset) AS tf
				) AS t,
			(	SELECT 	base32_decode(l.secret_key) AS secret_key
				FROM administratie.login AS l
				WHERE  l.login = for_login
				) AS sk
		) AS h,
	LATERAL (SELECT GET_BYTE(DECODE(CONCAT('0',RIGHT(h.hash,1)),'HEX'),0) AS _offset) AS l,
	LATERAL (SELECT SUBSTR(h.hash,1+l._offset*2,4*2) AS truncted_hash) AS th
;

IF (given_totp = ANY(_totp)) THEN
	RETURN TRUE;
ELSE
	RETURN FALSE;
END IF;

END;
$BODY$
LANGUAGE plpgsql;
