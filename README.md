Some functions / snippets of code that I think may be very useful to any postgres-user out there...


FUNCTION GET_OTP
================

AUTHOR:
-------
R. Wilmans-Sangwienwong (2019)

PURPOSE:
--------
Generate (a set of) TOTP, based on the SESSION_USER and the existence of that user in the table 'administratie.login.login' (easily changed to whatever) and
the corresponding secret key ('administratie.login.secret_key').
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

Example:
-- ---------------------------------------------------------------
SELECT get_otp(timeframe => 30, baa=>3);
-- ---------------------------------------------------------------
SELECT get_otp(timeframe => 30, baa=>1, test_key => 'JBSWY3DPEHPK3PXP' ); // Works well to test with http://blog.tinisles.com/2011/10/google-authenticator-one-time-password-algorithm-in-javascript/
-- --------------------------------------------------------------
SELECT  '123456' = ANY(get_otp())
-- --------------------------------------------------------------


FUNCTION GEN_RND_BASE32
=======================

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


FUNCTION TRANSPOSE
==================

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
SELECT transpose(	ds_in => 'raoul',
							   	ds_out => 'r_xpose',
							   	by_vars => 'rijnr',
							   	category_var => 'categorie');
SELECT * FROM r_xpose;
-- ---------------------------------------------------------------
SELECT transpose(	ds_in => 'raoul',
								category_var => 'categorie');
SELECT * FROM transpose_out;
-- ---------------------------------------------------------------
SELECT transpose(	ds_in => 'raoul', 
								by_vars => 'rijnr, categorie');
SELECT * FROM transpose_out;
-- ---------------------------------------------------------------

FUNCTION VERIFY_OTP
===================

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

FUNCTION BASE32_DECODE
======================

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
