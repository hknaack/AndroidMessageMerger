#!/bin/bash
# Android Message Merger
#
# This script helps to merge message threads, containing SMS and MMS, from an
# existing Android mmssms.db database into another such database.
#
# (c) 2018 Hartmut Knaack
#
# Tested Android versions:
#	Android 2.3 (source database)
#	Android 4.2.2 (source and destination database)
#
# Required software:
#	Bash (tested on 4.3.30)
#	SQlite (version 3.8.8 or newer)
#
# Recommended procedure:
# 1. Backup your phone data (preferably Nandroid)!
# 2. Restart your phone and make sure it will not connect to your phone network
#    (put into airplane mode or remove SIM card).
# 3. Copy your phones mmssms.db file (for example with adb pull from
#    /data/data/com.android.providers.telephony/databases/).
# 4. Do a dry run of this script to check if it recognizes your phone numbers
#    correctly. Use the -p and -r options to adjust the country prefix.
# 5. Run the script to merge your message threads from the source database into
#    the destination database.
# 6. Rename the mmssms.db file on your phone (mv mmssms.db mmssms.db.bak).
# 7. Copy the merged mmssms.db file back to your phone (for example with
#    adb push).
# 8. Adjust owner (chown radio:radio mmssms.db) and permission
#    (chmod 660 mmssms.db).
# 9. Reboot your phone.
# 10.Start message app

# Global variables
PROGNAME="$0"
BASENAME=$(basename ${PROGNAME})
INDB=
OUTDB=
BACKUP="true"
BACKUP_EXT=".bak"
DRYRUN="false"
CMP="cmp"
CMP_OPTS="-s"
SQLITEBIN="sqlite3"
FIFODIR="/tmp"
COLSEPARATOR='|'
LINESEPARATOR=$'\f'
PREFIX_PATTERN=
PREFIX_REPLACE=
DATE_ADJUST=3000

# Variables for the look-up table, which is built from the "canonical_addresses"
# table of the destination database and appended by entries of the source
# database, which do not exist in the destination database.
LUT_COUNT=0			# number of entries in look-up table
declare -a LUT_ID		# entries of _id coloumn
declare -a LUT_ADDRESS		# entries of address coloumn
declare -a LUT_ADDRESS_STRIPPED	# stripped down entries of address coloumn

# Variables for the translation tables
declare -a TTBL_ID		# translates the canonical_addresses _id of the
				# destination database the canonical_addresses
				# _id of the source database as index
declare -a TTBL_TID		# translates the thread-IDs by storing the
				# thread-ID of the destination database by using
				# the thread-ID of the source database as index

# Variables for the thread table hold in memory, which is built from the
# "threads" table of the destination database and appended by entries of the
# source database, which do not exist in the destination database.
THREAD_COUNT=0			# number of entries in threads table
declare -a THREAD_ID		# entries in _id coloumn
declare -a THREAD_DATE		# entries in date coloumn
declare -a THREAD_MCOUNT	# entries in message_count coloumn
declare -a THREAD_RID		# entries in recipient_ids coloumn
declare -a THREAD_SNIPPET	# entries in snippet coloumn
declare -a THREAD_SNIPPETCS	# entries in snippet_cs coloumn
declare -a THREAD_READ		# entries in read coloumn
declare -a THREAD_TYPE		# entries in type coloumn
declare -a THREAD_ERROR		# entries in error coloumn
declare -a THREAD_HASATTACHMENT	# entries in has_attachment coloumn
declare -a THREAD_NUM_MEMBERS	# amount of members in that thread

declare -a THREAD_LUT		# look-up table, which contains the indexes of
				# the in memory thread table, using the
				# thread-ID as index

# Print usage information of the script
#
# Usage:	usage
#
# returns:	exits with error code 1

usage() {
	cat <<EOF
Usage:
	$BASENAME [options]

	Merge a mmssms.db SQlite message database with an existing one.

Options:
-a		Adjust tolerance time between messages and their threads in ms
		(default value: 3000)
-B		Disable automatic backup of destination database (not
		recommended)
-c string	Coloumn separator used for SQlite database queries (should not
		be used in the database entries, default value: '|')
-d		Dry run. Just list the found phone numbers and its matches, then
		exit.
-f		Directory (with write permission) to use for temporary FIFO
		files (defaults to /tmp)
-h		Print this help text.
-i file.db	Filename of input SQlite database, where messages should be read
		from. (required option!)
-l string	Line separator used for SQlite database queries (should not be
		used in the database entries, default value: '\f')
-o file.db	Filename of SQlite database, into which messages should be
		merged. (required option!)
-p string	National phone number prefix to be replaced with -r (only works
		together with -r)
-r string	International prefix to replace the code given with -p (only
		works together with -p)

EOF

	exit 1
}

# Output error messages to stderr
#
# Usage:	echo_err "$string"
#
# $string:	string to be sent to stderr
# returns:	exit code of echo, usually 0

echo_err () {
	echo "$@" >&2
}

# Check a file for existence and read(/write) permission
#
# Usage:	filecheck "$filename" "$permission"
#
# $filename:	path and name of the file to check
# $permission:	string of file permission to check for. Currently, only "w" is
#		especially considered, "r" is always checked.
# returns:	0 if file exists and has got the requested permissions,
#		otherwise 1 and an error message is sent to stderr.

filecheck() {
	[[ -f "$1" ]] || {
		echo_err "Error: File $1 does not exist!"
		return 1
	}

	[[ -r "$1" ]] || {
		echo_err "Error: File $1 is not readable!"
		return 1
	}

	[[ "$2" == "w" && ! -w "$1" ]] && {
		echo_err "Error: File $1 is not writable!"
		return 1
	}

	return 0
}

# Check for existence and write permissions of the FIFO directory
#
# Usage:	fifodir "$dirname"
#
# $dirname:	path of the directory to check
# returns:	0 if directory exists and has got write permissions, otherwise
#		exits with code 1 and an error message is sent to stderr.

fifodir () {
	[[ -d "$1" ]] || {
		echo_err "Error: FIFO directory $1 does not exist!"
		exit 1
	}

	[[ -w "$1" ]] || {
		echo_err "Error: FIFO directory $1 is not writable!"
		exit 1
	}

	FIFODIR=$1
	return 0
}

# Create a FIFO, if it does not yet exist
#
# Usage:	create_fifo "$filename"
#
# $filename:	path and file name of the FIFO
# returns:	0 if FIFO was properly created, otherwise exits with code 1 and
#		an error message is sent to stderr

create_fifo () {
	[[ -p "$1" ]] || {
		mkfifo "$1" || {
			echo_err "Error: FIFO $1 could not be created!"
			exit 1
		}
	}

	[[ -p "$1" ]] || {
		echo_err "Error: FIFO $1 does not exist!"
		exit 1
	}

	[[ -r "$1" ]] || {
		echo_err "Error: FIFO $1 is not readable!"
		exit 1
	}

	[[ -w "$1" ]] || {
		echo_err "Error: FIFO $1 is not writable!"
		exit 1
	}

	return 0
}

# Check the input file for existence and read permission
#
# Usage:	infile "$filename"
#
# $filename:	path and file name to be checked
# returns:	0 if file exists and is readable, otherwise exits with code 1
#		and an error message is sent to stderr.

infile() {
	filecheck "$1" "r" || {
		echo_err "Error: Input file could not be opened!"
		exit 1
	}
	INDB="$1"

	return 0
}

# Check the output file for existence and write permission
#
# Usage:	outfile "$filename"
#
# $filename:	path and file name to be checked
# returns:	0 if file exists and is readable and writable, otherwise exits
#		with code 1 and an error message is sent to stderr.

outfile() {
	filecheck "$1" "w" || {
		echo_err "Error: Output file could not be opened!"
		exit 1
	}
	OUTDB="$1"

	return 0
}

# Clean up anything, that should not remain
#
# Usage:	cleanup
#
# returns:	0 on success

cleanup (){
	[[ -p "$INDBFIFO" ]] && rm "$INDBFIFO"
	[[ -p "$OUTDBFIFO" ]] && rm "$OUTDBFIFO"
}

# Set the DATE_ADJUST value
#
# Usage:	set_dateadjust "$number"
#
# $number:	integer value in ms
# returns:	sets DATE_ADJUST on success, otherwise exits showing usage
#		information.

set_dateadjust () {
	local number="$1"

	case $number in
		''|*[!0-9]*) usage
		;;
		*) DATE_ADJUST=$number
		;;
	esac
}

# Strip out phone number separators
#
# Usage:	stripped "$number"
#
# $number:	string containing a phone number
# returns:	stripped phone number on stdout

stripped () {
	local strip="$1"

	strip=${strip//" "}
	strip=${strip//"-"}
	strip=${strip//"/"}
	strip=${strip//"("}
	strip=${strip//")"}

	echo "$strip"
}

# Add international prefix to recognized phone numbers
#
# Usage:	international_prefix "$number"
#
# $number:	string of a plain phone number
# returns:	international phone number on stdout

international_prefix () {
	local number="$1"
	local len

	[[ ${number:0:2} == '00' ]] && number="${number/00/+}"

	[[ -n $PREFIX_PATTERN && -n $PREFIX_REPLACE ]] && {
		len=${#PREFIX_PATTERN}
		[[ ${number:0:${len}} == "$PREFIX_PATTERN" ]] &&
			number="${number/$PREFIX_PATTERN/$PREFIX_REPLACE}"
	}

	echo "$number"
}

# Try to convert phone number to international format
#
# Usage:	internationalized "$number"
#
# $number:	string of a phone number or address
# returns:	international phone number or address on stdout

internationalized () {
	local number="$1"

	case $number in
		''|*[!0-9]*) # NOP if it contains something else than digits
		;;
		* ) number=$(international_prefix "$number")
		;;
	esac

	echo "$number"
}

# Check if a given string is a phone number in international format
#
# Usage:	internationalnumber "$string"
#
# $string:	string of a phone number to check
# returns:	status code 0 and "true" on stdout on success, otherwise code 1
#		and "false" on stdout

internationalnumber () {
	local number="$1"

	[[ ${number:0:1} != '+' ]] && { echo "false"; return 1; }

	case ${number:1} in
		''|*[!0-9]*) echo "false"; return 1
		;;
		*)
		;;
	esac

	echo "true"
	return 0
}

# Minimum function
#
# Usage:	min "$first" "$second"
#
# $first:	numerical value
# $second:	numerical value
# returns:	smallest of the two values on stdout

min () {
	echo $(( $1 < $2 ? $1 : $2 ))
}

# Check if two given address strings match
#
# Usage:	matchaddr "$first" "$second" "$exact"
#
# $first:	first string containing an address (phone number)
# $second:	second string containing an address (phone number)
# $exact:	if set to "true", an exact match will be performed. Otherwise,
#		only the last characters will be compared (which should be
#		enough in most cases)
# returns:	status code 0 and "true" on stdout if both strings match,
#		otherwise code 1 and "false" on stdout

matchaddr () {
	local first="$1"
	local second="$2"
	local exact="$3"
	local delta first_len second_len min

	if [[ $exact == 'true' ]]
		then
			[[ "$first" == "$second" ]] && {
				echo 'true'
				return 0
			}
		else
			first_len=${#first}
			second_len=${#second}
			(( delta = first_len - second_len))
			[[ ${delta##-} -ge 3 ]] && {	# strips off negative sign to get absolute value
				echo 'false'		# string sizes differ too much
				return 1
			}
			min=$( min "$first_len" "$second_len" )
			[[ $min -gt 6 ]] && (( min -= 3))
			[[ "${first:(-$min)}" == "${second:(-$min)}" ]] && {
				echo 'true'
				return 0
			}
	fi

	echo 'false'
	return 1
}

# Replace single quotes (') by two single quotes ('') for SQL content
#
# Usage:	sqlquote "$string"
#
# $string:	string values to be used for SQL query
# returns:	quoted string on stdout

sqlquote () {
	echo "${1//\'/\'\'}"
}

# Count words in a string
#
# Usage:	wordcount "$string"
#
# $string:	string of words to count
# returns:	number of words on stdout

wordcount () {
	echo $(echo "$1" | wc -w)
}

# Sort words in a string
#
# Usage:	sortwords "$string"
#
# $string:	string of words to be sorted
# returns:	sorted string on stdout

sortwords () {
	local sorted=$(
		for el in $1
			do
				echo "$el"
			done | sort -n )
	echo "${sorted[@]}"
}

# Translate canonical ids
#
# Usage:	translate_cids "$members"
#
# $members:	string of canonical IDs to look up in translation table TTBL_ID
# returns:	string of translated canonical IDs on stdout

translate_cids () {
	local member
	local trans_member=$(
		for member in $1
			do
				echo "${TTBL_ID[$member]}"
			done)
	echo "${trans_member[@]}"
}

# Backup the destination database
#
# Usage:	backup_db
#
# returns:	0 on success, otherwise exits with code 1 and an error message
#		is sent to stderr.

backup_db () {
	local backupfile="${OUTDB}${BACKUP_EXT}"

	[[ -e "$backupfile" ]] && {
		echo_err "Error: backup file $backupfile already exists, can not backup"
		exit 1
	}
	cp "$OUTDB" "$backupfile"
	$("$CMP" "$CMP_OPTS" "$OUTDB" "$backupfile") || {
		echo_err "Error: Difference between $OUTDB and its backup $backupfile"
		exit 1
	}
}

# Assemble a wildcard WHERE-string from a list of coloumns and a string of
# characters to check for
#
# Usage:	assemble_wherestring "$coloumns" "$chars"
#
# $coloumns:	string of coloumn names separated by whitespaces
# $chars:	string of character sequence to check for in coloumns
# returns:	SQL WHERE-string, which checks for the character sequence in
# 		each mentioned coloumn, on stdout

assemble_wherestring () {
	local coloumns=$1
	local chars=$2
	local string quoted_chars col
	local col_count=0

	quoted_chars=$(sqlquote "$chars")
	string=$(
		for col in $coloumns
			do
				[[ $col_count -gt 0 ]] && echo -n "OR "
				echo -n "${col} LIKE '%${quoted_chars}%' "
				((col_count++))
			done)

	echo "${string[@]}"
}

# Check database tables coloumns for a character sequence
#
# Usage:	check_cols "$dbfile" "$table" "$coloumns" "$chars"
#
# $dbfile:	SQlite database file
# $table:	table name
# $coloumns:	string of space separated coloumns names to check
# $chars:	string of characters to check for
# returns:	amount of matches on stdout

check_cols () {
	local dbfile=$1
	local table=$2
	local coloumns=$3
	local chars=$4
	local count query where

	where=$(assemble_wherestring "$coloumns" "$chars")
	query="SELECT COUNT(${coloumns%% *}) FROM $table WHERE ${where};"
	count=$("$SQLITEBIN" "$dbfile" "$query")

	echo "$count"
}

# Check the canonical_addresses table for a character sequence
#
# Usage:	check_canonical_addresses "$dbfile" "$chars"
#
# $dbfile:	SQlite database file
# $chars:	string of characters to check for
# returns:	amount of matches on stdout

check_canonical_addresses () {
	local dbfile=$1
	local chars=$2
	local cols='_id address'

	echo $(check_cols "$dbfile" "canonical_addresses" "$cols" "$chars")
}

# Check the threads table for a character sequence
#
# Usage:	check_threads "$dbfile" "$chars"
#
# $dbfile:	SQlite database file
# $chars:	string of characters to check for
# returns:	amount of matches on stdout

check_threads () {
	local dbfile=$1
	local chars=$2
	local cols="_id date message_count recipient_ids snippet snippet_cs read \
		    type error has_attachment"

	echo $(check_cols "$dbfile" "threads" "$cols" "$chars")
}

# Check the sms table for a character sequence
#
# Usage:	check_sms "$dbfile" "$chars"
#
# $dbfile:	SQlite database file
# $chars:	string of characters to check for
# returns:	amount of matches on stdout

check_sms () {
	local dbfile=$1
	local chars=$2
	local cols="thread_id address person date protocol read status type \
		    reply_path_present subject body service_center locked \
		    error_code seen"

	echo $(check_cols "$dbfile" "sms" "$cols" "$chars")
}

# Check the canonical_addresses, threads and sms table for a character sequence
#
# Usage:	check_db "$dbfile" "$chars"
#
# $dbfile:	SQlite database file
# $chars:	string of characters to check for
# returns:	amount of matches on stdout

check_db () {
	local dbfile=$1
	local chars=$2
	local matches=0
	local ret

	ret=$(check_canonical_addresses "$dbfile" "$chars")
	((matches += ret))

	ret=$(check_threads "$dbfile" "$chars")
	((matches += ret))

	ret=$(check_sms "$dbfile" "$chars")
	((matches += ret))

	echo "$matches"
}

# Check the database for entries containing the used coloumn or line separator
#
# Usage:	check_db_separators "$dbfile"
#
# $dbfile:	SQlite database file
# returns:	code 0 if the database entries do not contain a used separator
#		string, otherwise exit with code 1 and print a message to stderr

check_db_separators () {
	local dbfile=$1
	local ret

	ret=$(check_db "$dbfile" "$COLSEPARATOR")
	[[ $ret -gt 0 ]] && {
		echo_err "Error: database $dbfile has got $ret entries containing coloumn separator '$COLSEPARATOR'. Use the -c option to specify a different one!"
		exit 1
	}

	ret=$(check_db "$dbfile" "$LINESEPARATOR")
	[[ $ret -gt 0 ]] && {
		echo_err "Error: database $dbfile has got $ret entries containing line separator '$LINESEPARATOR'. Use the -l option to specify a different one!"
		exit 1
	}

	return 0
}

# Create lookup table from canonical_addresses (destination database) containing
# _id, address and stripped down phone numbers in international format
# (separators removed).
#
# Usage:	create_lut
#
# returns:	code 0 on success, otherwise any error code
# global vars:	LUT_ID[], LUT_ADDRESS[], LUT_ADDRESS_STRIPPED[], LUT_COUNT

create_lut () {
	local id address
	local query="SELECT _id, address FROM canonical_addresses ORDER BY _id;"

	"$SQLITEBIN" "$OUTDB" -newline "$LINESEPARATOR" -separator "$COLSEPARATOR" "$query" > "$OUTDBFIFO" &

	while IFS=$COLSEPARATOR read -r -d "$LINESEPARATOR" id address
		do
			LUT_ID["$LUT_COUNT"]=$id
			LUT_ADDRESS["$LUT_COUNT"]=$address
			LUT_ADDRESS_STRIPPED["$LUT_COUNT"]=$(internationalized $(stripped "$address"))
			((LUT_COUNT++))
		done < "$OUTDBFIFO"
}

# Add entry to destination database and update the lookup table.
#
# Usage:	add_dest_address "$address" "$can_addr_stripped" "$canonicalid"
#
# $address:	canonical address to be added to destination database
# $can_addr_stripped:	stripped canonical address to be added to lookup table
# $canonicalid:	source canonical ID of the entry
#
# returns:	code 0 on success, otherwise any error code
# global vars:	LUT_ID[], LUT_ADDRESS[], LUT_ADDRESS_STRIPPED[], LUT_COUNT,
#		TTBL_ID

add_dest_address () {
	local new_addr=$1
	local can_addr_stripped=$2
	local canonicalid=$3
	local query

	query="INSERT INTO canonical_addresses (address) VALUES ('${new_addr}');"
	"$SQLITEBIN" "$OUTDB" "$query"

# Query the destination database for the _id of this new entry and add a new
# entry to the lookup table
	query="SELECT _id FROM canonical_addresses WHERE address='${new_addr}';"
	LUT_ID["$LUT_COUNT"]=$("$SQLITEBIN" "$OUTDB" "$query")
	LUT_ADDRESS["$LUT_COUNT"]=$new_addr
	LUT_ADDRESS_STRIPPED["$LUT_COUNT"]=$can_addr_stripped
	TTBL_ID["$canonicalid"]=${LUT_ID[$LUT_COUNT]}
	echo "address $new_addr was added to outfile, its new _id is ${LUT_ID[$LUT_COUNT]}"
	((LUT_COUNT++))
}

# Synchronize entry of the source database with existing entries in the lookup
# table by populating a translation table.
#
# Usage:	sync_address "$canonicalid" "$address"
#
# $canonicalid:	source canonical ID of the entry
# $address:	canonical address to be added to destination database
#
# returns:	code 0 if the destination database has not been changed,
#		otherwise code 1
#
# global vars:	LUT_ID[], LUT_ADDRESS_STRIPPED[], LUT_COUNT, TTBL_ID[]

sync_address () {
	local canonicalid=$1
	local canonicaladdress=$2
	local can_addr_stripped int_num i ret

# Convert canonical address to international format
	can_addr_stripped=$(internationalized $(stripped "$canonicaladdress"))
	int_num=$(internationalnumber "$can_addr_stripped")

# Search through LUT_ADDRESS_STRIPPED for this address
	for ((i = 0; i < LUT_COUNT; i++))
		do
			ret=$(matchaddr "$can_addr_stripped" "${LUT_ADDRESS_STRIPPED[$i]}" "$int_num")
# On match, add entry to translation table, continue with next entry
			[[ "$ret" == 'true' ]] && {
				TTBL_ID["$canonicalid"]=${LUT_ID[$i]}
				echo "_id $canonicalid ($canonicaladdress) from infile matches _id ${LUT_ID[$i]} (${LUT_ADDRESS[$i]}) from outfile"
				return 0
			}
		done

# No match: add address (in international format, if it is a phone number) to
# destination database
	echo "_id $canonicalid ($canonicaladdress) did not match any entry in outfile"
	if [[ "$int_num" == 'true' ]]
		then
			new_addr=$(sqlquote "$can_addr_stripped")
		else
			new_addr=$(sqlquote "$canonicaladdress")
	fi

	[[ "$DRYRUN" == 'true' ]] && return 0

	add_dest_address "$new_addr" "$can_addr_stripped" "$canonicalid"
	return 1
}

# Synchronize entries of the source database with existing entries in the lookup
# table by populating a translation table. Non-existing entries will be added to
# both, the lookup table and (unless doing a dry-run) the destination database.
#
# Usage:	sync_src_addresses
#
# returns:	code 0 on success, otherwise any error code

sync_src_addresses () {
	local query canonicalid canonicaladdress

	query="SELECT _id, address FROM canonical_addresses ORDER BY _id;"
	"$SQLITEBIN" "$INDB" -newline "$LINESEPARATOR" -separator "$COLSEPARATOR" "$query" > "$INDBFIFO" &

	while IFS=$COLSEPARATOR read -r -d "$LINESEPARATOR" canonicalid canonicaladdress
		do
			sync_address "$canonicalid" "$canonicaladdress"
		done < "$INDBFIFO"
}

# Create thread table in memory from the destination database
#
# Usage:	create_thread_table
#
# returns:	Code 0 on success, otherwise any error code.
#
# global vars:	THREAD_ID[], THREAD_DATE[], THREAD_MCOUNT[], THREAD_RID[],
#		THREAD_SNIPPET[], THREAD_SNIPPETCS[], THREAD_READ[],
#		THREAD_TYPE[], THREAD_ERROR[], THREAD_HASATTACHMENT[],
#		THREAD_NUM_MEMBERS[], THREAD_LUT[], THREAD_COUNT

create_thread_table () {
	local query id date mcount rid snippet snippetcs read type error
	local has_attachment

	query="SELECT _id, date, message_count, recipient_ids, snippet, \
	       snippet_cs, read, type, error, has_attachment \
	       FROM threads \
	       ORDER BY _id ASC;"
	"$SQLITEBIN" "$OUTDB" -newline "$LINESEPARATOR" -separator "$COLSEPARATOR" "$query" > "$OUTDBFIFO" &

	while IFS=$COLSEPARATOR read -r -d "$LINESEPARATOR" id date mcount rid snippet snippetcs read type error has_attachment
		do
		THREAD_ID["$THREAD_COUNT"]=$id
		THREAD_DATE["$THREAD_COUNT"]=$date
		THREAD_MCOUNT["$THREAD_COUNT"]=$mcount
		THREAD_RID["$THREAD_COUNT"]=$rid
		THREAD_SNIPPET["$THREAD_COUNT"]=$snippet
		THREAD_SNIPPETCS["$THREAD_COUNT"]=$snippetcs
		THREAD_READ["$THREAD_COUNT"]=$read
		THREAD_TYPE["$THREAD_COUNT"]=$type
		THREAD_ERROR["$THREAD_COUNT"]=$error
		THREAD_HASATTACHMENT["$THREAD_COUNT"]=$has_attachment
		THREAD_NUM_MEMBERS["$THREAD_COUNT"]=$(wordcount "$rid")
		THREAD_LUT["$id"]="$THREAD_COUNT"
		((THREAD_COUNT++))
	done < "$OUTDBFIFO"
}

# Find a matching thread, which contains the same (translated) recipient IDs as
# the existing thread from the destination database
#
# Usage:	match_threads "$id" "$num" "$rid"
#
# $id:		source thread ID
# $num:		number of recipient IDs of the source thread
# $rid:		string of already translated and sorted recipient IDs of the
#		source thread
#
# returns:	Code 0 if a matching thread has been found, otherwise 1. Also
#		outputs, which thread matches and which recipient IDs are found.
#
# global vars:	TTBL_TID[]

match_threads () {
	local i
	local id=$1
	local num_members=$2
	local rid=$3

	for ((i = 0; i < THREAD_COUNT; i++))
		do
# Make sure to have the same amount of members before looking closer into it
			[[ $num_members -eq ${THREAD_NUM_MEMBERS[$i]} ]] && {
				[[ "$rid" == $(sortwords "${THREAD_RID[$i]}") ]] && {
					TTBL_TID["$id"]=${THREAD_ID[$i]}
					echo -e "Thread matches with ${THREAD_ID[$i]} because source recipient_ids '${rid}' match destinations '${THREAD_RID[$i]}'"
					return 0
					}
				}
		done

	return 1
}

# Add a new thread to the destination database
#
# Usage:	add_dest_thread "$date" "$rid" "$snippet" "$snippet_cs" "$read" "$type" "$error" "$has_attachment"
#
# $date:	date of latest thread entry (Unix time)
# $rid:		string of recipient IDs
# $snippet:	string of thread snippet (body of latest entry)
# $snippet_cs:	string of thread snippet_cs
# $read:	integer indicating read status of the thread
# $type:	integer indicating the type of the thread
# $error:	integer error code of the thread
# $has_attachment: integer indicating attachments of the thread
#
# returns:	Code 0 on success, otherwise any error code

add_dest_thread () {
	local date=$(sqlquote "$1")
	local mcount=0 # a trigger will update this on every insert
	local rid=$(sqlquote "$2")
	local snippet=$(sqlquote "$3")
	local snippet_cs=$(sqlquote "$4")
	local read=$(sqlquote "$5")
	local type=$(sqlquote "$6")
	local error=$(sqlquote "$7")
	local has_attachment=$(sqlquote "$8")
	local query

	query="INSERT INTO threads (date, message_count, recipient_ids, \
				    snippet, snippet_cs, read, type, error, \
				    has_attachment) \
	       VALUES ('${date}', '${mcount}', '${rid}', '${snippet}', \
		       '${snippetcs}', '${read}', '${type}', '${error}', \
		       '${has_attachment}');"

	"$SQLITEBIN" "$OUTDB" "$query"
}

# Synchonize threads from the source database with threads from the destination
# database
#
# Usage:	sync_src_threads
#
# returns:	Code 0 on success, otherwise any error code
#
# global vars:	THREAD_ID[], THREAD_DATE[], THREAD_MCOUNT[], THREAD_RID[],
#		THREAD_SNIPPET[], THREAD_SNIPPETCS[], THREAD_READ[],
#		THREAD_TYPE[], THREAD_ERROR[], THREAD_HASATTACHMENT[],
#		THREAD_NUM_MEMBERS[], THREAD_LUT[], TTBL_ID[], THREAD_COUNT

sync_src_threads () {
	local query id date mcount rid snippet snippetcs read type error
	local has_attachment num_members tr_rid q_rid

	query="SELECT _id, date, message_count, recipient_ids, snippet, \
	       snippet_cs, read, type, error, has_attachment \
	       FROM threads \
	       ORDER BY _id ASC;"
	"$SQLITEBIN" "$INDB" -newline "$LINESEPARATOR" -separator "$COLSEPARATOR" "$query" > "$INDBFIFO" &

	while IFS=$COLSEPARATOR read -r -d "$LINESEPARATOR" id date mcount rid snippet snippetcs read type error has_attachment
		do
			echo "-------------------------"
			num_members=$(wordcount "$rid")
			echo -e "TID: $id TDate: $date TRead: $read TSnippet: ${snippet:0:10} TRID: $rid TMembers: $num_members"
			tr_rid=$(translate_cids "$rid")
			tr_rid=$(sortwords "$tr_rid")

			match_threads "$id" "$num_members" "$tr_rid" && continue

# No thread match: copy entry to destination database
			add_dest_thread "$date" "$tr_rid" "$snippet" "$snippetcs" "$read" "$type" "$error" "$has_attachment"

# Query the destination database for the thread_id of this new entry and add it
# to the lookup table
			q_rid=$(sqlquote "$tr_rid")
			query="SELECT _id FROM threads WHERE recipient_ids='${q_rid}';"
			THREAD_ID["$THREAD_COUNT"]=$("$SQLITEBIN" "$OUTDB" "$query")
			TTBL_TID["$id"]=${THREAD_ID["$THREAD_COUNT"]}
			THREAD_DATE["$THREAD_COUNT"]=$date
			THREAD_MCOUNT["$THREAD_COUNT"]=$mcount # set to zero for starters or keep for reference?
			THREAD_RID["$THREAD_COUNT"]=$rid
			THREAD_SNIPPET["$THREAD_COUNT"]=$snippet
			THREAD_SNIPPETCS["$THREAD_COUNT"]=$snippetcs
			THREAD_READ["$THREAD_COUNT"]=$read
			THREAD_TYPE["$THREAD_COUNT"]=$type
			THREAD_ERROR["$THREAD_COUNT"]=$error
			THREAD_HASATTACHMENT["$THREAD_COUNT"]=$has_attachment
			THREAD_NUM_MEMBERS["$THREAD_COUNT"]=$num_members
			THREAD_LUT["${THREAD_ID[$THREAD_COUNT]}"]=$THREAD_COUNT
			echo -e "no thread match found, adding to destination: TID: ${THREAD_ID[$THREAD_COUNT]} TDate: ${THREAD_DATE[$THREAD_COUNT]} TRead: ${THREAD_READ[$THREAD_COUNT]} TSnippet: ${THREAD_SNIPPET[$THREAD_COUNT]:0:10} TRID: ${THREAD_RID[$THREAD_COUNT]} TMembers: $(wordcount ${THREAD_RID[$THREAD_COUNT]})"
			((THREAD_COUNT++))
		done < "$INDBFIFO"
}

# Add a message to the destination database
#
# Usage:	add_dest_message "$tid" "$address" "$person" "$date" "$protocol" "$read" "$status" "$type" "$rpp" "$subject" "$body" "$scenter" "$locked" "$errcode" "$seen"
#
# $tid:		thread ID in the destination database
# $address:	address of the message
# $person:	person entry of the message
# $date:	date of the message
# $protocol:	protocol of the message
# $read:	read status of the message
# $status:	status of the message
# $type:	type of the message
# $rpp:		reply_path_present field of the message
# $subject:	subject of the message
# $body:	content body of the message
# $scenter:	service_center entry of the message
# $locked:	locked flag of the message
# $errcode:	error code of the message
# $seen:	seen flag of the message
#
# returns:	Code 0 on success, otherwise any error code. Prints some
#		information on stdout.

add_dest_message () {
	local tid=$(sqlquote "$1")
	local address=$(sqlquote "$2")
	local person=$(sqlquote "$3")
	local date=$(sqlquote "$4")
	local protocol=$(sqlquote "$5")
	local read=$(sqlquote "$6")
	local status=$(sqlquote "$7")
	local type=$(sqlquote "$8")
	local rpp=$(sqlquote "$9")
	local subject=$(sqlquote "${10}")
	local body=$(sqlquote "${11}")
	local scenter=$(sqlquote "${12}")
	local locked=$(sqlquote "${13}")
	local err_code=$(sqlquote "${14}")
	local seen=$(sqlquote "${15}")
	local query="INSERT INTO sms (thread_id, address, person, date, \
				      protocol, read, status, type, \
				      reply_path_present, subject, body, \
				      service_center, locked, error_code, seen) \
		     VALUES ('${tid}', '${address}', '${person}', '${date}', \
			     '${protocol}', '${read}', '${status}', '${type}', \
			     '${rpp}', '${subject}', '${body}', '${scenter}', \
			     '${locked}', '${err_code}', '${seen}');"

	echo -e "Adding message to destination. TID: $tid Date: $date Address: $address Subject: '${subject:0:10}' Body: '${body:0:10}'"

	"$SQLITEBIN" "$OUTDB" "$query"
}

# Write back cached thread data to destination database
#
# Usage:	writeback_dest_thread "$entry" "$mdate"
#
# $entry:	index of the thread table entry
# $mdate:	date of the reference message
#
# returns:	Code 0 on success, otherwise any error code. Prints some
#		information on stdout.

writeback_dest_thread () {
	local entry=$1
	local mdate=$2
	local tid=$(sqlquote "${THREAD_ID[$entry]}")
	local tdate=$(sqlquote "${THREAD_DATE[$entry]}")
	local snippet=$(sqlquote "${THREAD_SNIPPET[$entry]}")
	local snippetcs=$(sqlquote "${THREAD_SNIPPETCS[$entry]}")
	local read=$(sqlquote "${THREAD_READ[$entry]}")
	local type=$(sqlquote "${THREAD_TYPE[$entry]}")
	local error=$(sqlquote "${THREAD_ERROR[$entry]}")
	local hasattachment=$(sqlquote "${THREAD_HASATTACHMENT[$entry]}")
	local query="UPDATE threads \
		     SET date='${tdate}', snippet='${snippet}', \
			 snippet_cs='${snippetcs}', read='${read}', \
			 type='${type}', error='${error}', \
			 has_attachment='${hasattachment}' \
		     WHERE _id='${tid}';"

	echo -e "Destination T-Date ${THREAD_DATE[$entry]} newer than adjusted M-Date $((mdate + DATE_ADJUST)), writing back. '${snippet:0:10}...'"

	"$SQLITEBIN" "$OUTDB" "$query"
}

# Write back thread date to destination database
#
# Usage:	write_dest_thread_date "$entry" "$date"
#
# $entry:	index of the thread table entry
# $date:	date of the thread
#
# returns:	Code 0 on success, otherwise any error code

write_dest_thread_date () {
	local tid=$(sqlquote "${THREAD_ID[$1]}")
	local date=$(sqlquote "$2")
	local query="UPDATE threads SET date='${date}' WHERE _id='${tid}';"

	"$SQLITEBIN" "$OUTDB" "$query"
}

# Read back thread from destination database to the cached threads table
#
# Usage:	readback_dest_thread "$entry" "$mdate"
#
# $entry:	index of the thread table entry
# $mdate:	date of the reference message
#
# returns:	Code 0 on success, otherwise any error code. Prints some
#		information on stdout, error messages will be sent to stderr.
#
# global vars:	THREAD_DATE[], THREAD_MCOUNT[], THREAD_SNIPPET[], THREAD_READ[],
#		THREAD_SNIPPETCS[], THREAD_TYPE[], THREAD_ERROR[],
#		THREAD_HASATTACHMENT[]

readback_dest_thread () {
	local entry=$1
	local mdate=$2
	local tid=$(sqlquote "${THREAD_ID[$entry]}")
	local old_tdate=${THREAD_DATE["$entry"]}
	local count=0
	local date mcount snippet snippetcs read type error hasattachment
	local query="SELECT date, message_count, snippet, snippet_cs, read, \
			    type, error, has_attachment \
		     FROM threads \
		     WHERE _id='${tid}';"

	"$SQLITEBIN" $OUTDB -newline "$LINESEPARATOR" -separator "$COLSEPARATOR" "$query" > "$OUTDBFIFO" &

	while IFS=$COLSEPARATOR read -r -d "$LINESEPARATOR" date mcount snippet snippetcs read type error hasattachment
		do
			[[ $((count++)) -ge 1 ]] && {
				echo_err "Error reading thread entry. count should be 1, but is ${count}"
				exit 1
			}
			THREAD_DATE["$entry"]=$date
			THREAD_MCOUNT["$entry"]=$mcount
			THREAD_SNIPPET["$entry"]=$snippet
			THREAD_SNIPPETCS["$entry"]=$snippetcs
			THREAD_READ["$entry"]=$read
			THREAD_TYPE["$entry"]=$type
			THREAD_ERROR["$entry"]=$error
			THREAD_HASATTACHMENT["$entry"]=$hasattachment
			echo -e "Destination Thread date $old_tdate is older than adjusted message date $((mdate + DATE_ADJUST)), reading in snippet '${snippet:0:10}'"
		done < "$OUTDBFIFO"
}

# Resynchronize destination threads table with the cached threads table in
# memory
#
# Usage:	resync_thread "$entry" "$mdate"
#
# $entry:	index of the threads table entry
# $mdate:	date of the reference message
#
# returns:	Code 0 on success, otherwise any error code.

resync_thread () {
	local entry=$1
	local mdate=$2

	if [[ ${THREAD_DATE["$entry"]} -ge $((mdate + DATE_ADJUST)) ]]
		then
# If sms date is lower than thread date, write back cached thread entry
			writeback_dest_thread "$entry" "$mdate"
		else
# Otherwise overwrite thread date and read in new thread content
			write_dest_thread_date "$entry" "$mdate"
			readback_dest_thread "$entry" "$mdate"
	fi
}

# Merge messages from the source database with existing messages in the
# destination database
#
# Usage:	merge_messages
#
# returns:	Code 0 on success, otherwise any error code. May output some
#		progress information on stdout.

merge_messages () {
	local tid address person date protocol read status type error_code
	local reply_path_present subject body service_center locked seen out_tid
	local query="SELECT thread_id, address, person, date, protocol, read, \
			    status, type, reply_path_present, subject, body, \
			    service_center, locked, error_code, seen \
		     FROM sms;"

	"$SQLITEBIN" "$INDB" -newline "$LINESEPARATOR" -separator "$COLSEPARATOR" "$query" > "$INDBFIFO" &

	while IFS=$COLSEPARATOR read -r -d "$LINESEPARATOR" tid address person date protocol read status type reply_path_present subject body service_center locked error_code seen
		do
			[[ -z "$address" ]] && {
				echo "Skipping message with empty Address containing '${body:0:20}...'"
				continue
			}
			[[ -z "$tid" ]] && {
				echo "Skipping message with empty Thread_ID containing '${body:0:20}...'"
				continue
			}

# Add entry to table sms
			echo "-------------------------------------"
			out_tid=${TTBL_TID["$tid"]}
			add_dest_message "$out_tid" "$address" "$person" "$date" "$protocol" "$read" "$status" "$type" "$reply_path_present" "$subject" "$body" "$service_center" "$locked" "$error_code" "$seen"

			resync_thread "${THREAD_LUT[$out_tid]}" "$date"
# Next entry of source database
		done < "$INDBFIFO"
}

# Dump IDs and stripped addresses from the lookup table of canonical adddresses.
#
# Usage:	dump_lut
#
# returns:	Code 0 on success, otherwise any error code. Outputs canonical
#		ID and stripped addresses on stdout.

dump_lut () {
	local i

	for ((i = 0; i < LUT_COUNT; i++))
		do
			echo -e "LuT ID: ${LUT_ID[$i]} \t Stripped Address: ${LUT_ADDRESS_STRIPPED[$i]}"
		done
}

# Dump content of threads table
#
# Usage:	dump_threads
#
# returns:	Code 0 on success, otherwise any error code. Outputs thread
#		content on stdout.

dump_threads () {
	local i

	for ((i = 0; i < THREAD_COUNT; i++))
		do
			echo -e "TID: ${THREAD_ID[$i]} TDate: ${THREAD_DATE[$i]} TRead: ${THREAD_READ[$i]} TSnippet: ${THREAD_SNIPPET[$i]:0:10} TRID: ${THREAD_RID[$i]} TMembers: $(wordcount ${THREAD_RID[$i]})"
		done
}

# Check parameters
# Print usage if run without options
[[ $# -eq 0 ]] && usage

# Parse options
while getopts ":a:Bc:df:hi:l:o:p:r:" option
	do
		case "$option" in
			a) set_dateadjust "$OPTARG"
			;;
			B) BACKUP="false"
			;;
			c) COLSEPARATOR="$OPTARG"
			;;
			d) DRYRUN="true"
			;;
			f) FIFODIR="$OPTARG"
			;;
			h|\?) usage
			;;
			i) infile "$OPTARG"
			;;
			l) LINESEPARATOR="$OPTARG"
			;;
			o) outfile "$OPTARG"
			;;
			p) PREFIX_PATTERN="$OPTARG"
			;;
			r) PREFIX_REPLACE="$OPTARG"
			;;
		esac
	done
shift $(($OPTIND - 1))

[[ -z $INDB || -z $OUTDB ]] && usage

[[ ( -n $PREFIX_PATTERN && -z $PREFIX_REPLACE ) ||
   ( -z $PREFIX_PATTERN && -n $PREFIX_REPLACE)]] && usage

# Backup destination database, unless disabled
[[ $BACKUP == "true" ]] && backup_db

# Check if selected coloumn separator and line separator are unused in database
# entries.
check_db_separators "$INDB"
check_db_separators "$OUTDB"

# Check if FIFO directory can be used
fifodir "$FIFODIR"

OUTDBFIFO=${FIFODIR}"/outdb.fifo"
create_fifo "$OUTDBFIFO"

# Create lookup table from canonical_addresses
create_lut

# Dump content of lookup table for debugging
dump_lut

# Query source database, cycle through entries
INDBFIFO=${FIFODIR}"/indb.fifo"
create_fifo "$INDBFIFO"

sync_src_addresses

[[ "$DRYRUN" == 'true' ]] && {
	cleanup
	exit 0
}

# Get a local copy of table threads of destination database
create_thread_table

# Dump content of threads table for debugging
dump_threads

# Compare entries of table threads of source database and add them to
# destination database if needed
echo "Processing threads from source database"
sync_src_threads

# Read and process all messages from source database
merge_messages

echo "Merging done, cleaning up."

# Clean up
cleanup
