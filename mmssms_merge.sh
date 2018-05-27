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

PROGNAME="$0"
BASENAME=$(basename ${PROGNAME})
INDB=
OUTDB=
BACKUP="true"
BACKUP_EXT=".bak"
DRYRUN="false"
SQLITEBIN="sqlite3"
FIFODIR="/tmp"
COLSEPARATOR='|'
LINESEPARATOR=$'\f'
PREFIX_PATTERN=
PREFIX_REPLACE=
DATE_ADJUST=3000

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
[[ $BACKUP == "true" ]] && {
	BACKUPFILE="${OUTDB}${BACKUP_EXT}"
	[[ -e "$BACKUPFILE" ]] && {
		echo_err "Error: backup file $BACKUPFILE already exists, can not backup"
		exit 1
	}
	cp "$OUTDB" "$BACKUPFILE"
	$(cmp -s "$OUTDB" "$BACKUPFILE") || {
		echo_err "Error: Difference between $OUTDB and its backup $BACKUPFILE"
		exit 1
	}
}

# Check if selected coloumn separator and line separator are unused in database
# entries.
check_db_separators "$INDB"
check_db_separators "$OUTDB"

# Check if FIFO directory can be used
fifodir "$FIFODIR"

# Create lookup table from canonical_addresses (destination database) containing
# _id, address and stripped down phone numbers in international format
# (separators removed)
OUTDBFIFO=${FIFODIR}"/outdb.fifo"
create_fifo "$OUTDBFIFO"

LUT_COUNT=0
QUERY="SELECT _id, address FROM canonical_addresses ORDER BY _id;"
"$SQLITEBIN" "$OUTDB" -newline "$LINESEPARATOR" -separator "$COLSEPARATOR" "$QUERY" > "$OUTDBFIFO" &

while IFS=$COLSEPARATOR read -r -d "$LINESEPARATOR" CANONICALID CANONICALADDRESS
	do
		LUT_ID["$LUT_COUNT"]=$CANONICALID
		LUT_ADDRESS["$LUT_COUNT"]=$CANONICALADDRESS
		LUT_ADDRESS_STRIPPED["$LUT_COUNT"]=$(internationalized $(stripped "$CANONICALADDRESS"))
		((LUT_COUNT++))
	done < "$OUTDBFIFO"

# Dump content of lookup table for debugging
for ((i = 0; i < LUT_COUNT; i++))
	do
		echo -e "LuT ID: ${LUT_ID[$i]} \t Stripped Address: ${LUT_ADDRESS_STRIPPED[$i]}"
	done

# Query source database, cycle through entries
INDBFIFO=${FIFODIR}"/indb.fifo"
create_fifo "$INDBFIFO"

QUERY="SELECT _id, address FROM canonical_addresses ORDER BY _id;"
"$SQLITEBIN" "$INDB" -newline "$LINESEPARATOR" -separator "$COLSEPARATOR" "$QUERY" > "$INDBFIFO" &

while IFS=$COLSEPARATOR read -r -d "$LINESEPARATOR" CANONICALID CANONICALADDRESS
	do
# Convert CANONICALADDRESS to international format
		CAN_ADDR_STRIPPED=$(internationalized $(stripped "$CANONICALADDRESS"))
		INT_NUM=$(internationalnumber "$CAN_ADDR_STRIPPED")

# Search through LUT_ADDRESS_STRIPPED for this address
		for ((i = 0; i < LUT_COUNT; i++))
			do
				RET=$(matchaddr "$CAN_ADDR_STRIPPED" "${LUT_ADDRESS_STRIPPED[$i]}" "$INT_NUM")
# On match, add entry to translation table, continue with next entry
				[[ "$RET" == 'true' ]] && {
					TTBL_ID["$CANONICALID"]=${LUT_ID[$i]}
					echo "_id $CANONICALID ($CANONICALADDRESS) from infile matches _id ${LUT_ID[$i]} (${LUT_ADDRESS[$i]}) from outfile"
					continue 2
				}
			done

# No match: add address (in international format, if it is a phone number) to
# destination database
		echo "_id $CANONICALID ($CANONICALADDRESS) did not match any entry in outfile"
		if [[ "$INT_NUM" == 'true' ]]
			then
				NEW_ADDR=$(sqlquote "$CAN_ADDR_STRIPPED")
			else
				NEW_ADDR=$(sqlquote "$CANONICALADDRESS")
		fi

		[[ "$DRYRUN" == 'false' ]] && {
			QUERY="INSERT INTO canonical_addresses (address) VALUES ('${NEW_ADDR}');"
			"$SQLITEBIN" "$OUTDB" "$QUERY"

# Query the destination database for the _id of this new entry and add a new
# entry to the lookup table
			QUERY="SELECT _id \
			       FROM canonical_addresses \
			       WHERE address='${NEW_ADDR}';"
			LUT_ID["$LUT_COUNT"]=$("$SQLITEBIN" "$OUTDB" "$QUERY")
			LUT_ADDRESS["$LUT_COUNT"]=$NEW_ADDR
			LUT_ADDRESS_STRIPPED["$LUT_COUNT"]=$CAN_ADDR_STRIPPED
			TTBL_ID["$CANONICALID"]=${LUT_ID[$LUT_COUNT]}
			echo "address $NEW_ADDR was added to outfile, its new _id is ${LUT_ID[$LUT_COUNT]}"
			((LUT_COUNT++))
		}
done < "$INDBFIFO"

[[ "$DRYRUN" == 'true' ]] && {
	cleanup
	exit 0
}

# Get a local copy of table threads of destination database
THREAD_COUNT=0
QUERY="SELECT _id, date, message_count, recipient_ids, snippet, snippet_cs, \
	      read, type, error, has_attachment \
       FROM threads \
       ORDER BY _id ASC;"
"$SQLITEBIN" "$OUTDB" -newline "$LINESEPARATOR" -separator "$COLSEPARATOR" "$QUERY" > "$OUTDBFIFO" &

while IFS=$COLSEPARATOR read -r -d "$LINESEPARATOR" T_ID T_DATE T_MCOUNT T_RID T_SNIPPET T_SNIPPETCS T_READ T_TYPE T_ERROR T_HASATTACHMENT
	do
		THREAD_ID["$THREAD_COUNT"]=$T_ID
		THREAD_DATE["$THREAD_COUNT"]=$T_DATE
		THREAD_MCOUNT["$THREAD_COUNT"]=$T_MCOUNT
		THREAD_RID["$THREAD_COUNT"]=$T_RID
		THREAD_SNIPPET["$THREAD_COUNT"]=$T_SNIPPET
		THREAD_SNIPPETCS["$THREAD_COUNT"]=$T_SNIPPETCS
		THREAD_READ["$THREAD_COUNT"]=$T_READ
		THREAD_TYPE["$THREAD_COUNT"]=$T_TYPE
		THREAD_ERROR["$THREAD_COUNT"]=$T_ERROR
		THREAD_HASATTACHMENT["$THREAD_COUNT"]=$T_HASATTACHMENT
		THREAD_NUM_MEMBERS["$THREAD_COUNT"]=$(wordcount "$T_RID")
		THREAD_LUT["$T_ID"]="$THREAD_COUNT"
		((THREAD_COUNT++))
	done < "$OUTDBFIFO"

# Dump content of threads table for debugging
for ((i = 0; i < THREAD_COUNT; i++))
	do
		echo -e "TID: ${THREAD_ID[$i]} TDate: ${THREAD_DATE[$i]} TRead: ${THREAD_READ[$i]} TSnippet: ${THREAD_SNIPPET[$i]:0:10} TRID: ${THREAD_RID[$i]} TMembers: $(wordcount ${THREAD_RID[$i]})"
	done

# Compare entries of table threads of source database and add them to
# destination database if needed
echo "Processing threads from source database"
QUERY="SELECT _id, date, message_count, recipient_ids, snippet, snippet_cs, \
	      read, type, error, has_attachment \
       FROM threads \
       ORDER BY _id ASC;"
"$SQLITEBIN" "$INDB" -newline "$LINESEPARATOR" -separator "$COLSEPARATOR" "$QUERY" > "$INDBFIFO" &

while IFS=$COLSEPARATOR read -r -d "$LINESEPARATOR" T_ID T_DATE T_MCOUNT T_RID T_SNIPPET T_SNIPPETCS T_READ T_TYPE T_ERROR T_HASATTACHMENT
	do
		echo "-------------------------"
		IN_THREAD_NUM_MEMBERS=$(wordcount "$T_RID")
		echo -e "TID: $T_ID TDate: $T_DATE TRead: $T_READ TSnippet: ${T_SNIPPET:0:10} TRID: $T_RID TMembers: $IN_THREAD_NUM_MEMBERS"
		TR_RID=$(translate_cids "$T_RID")
		TR_RID=$(sortwords "$TR_RID")
		for ((i = 0; i < THREAD_COUNT; i++))
			do
# Make sure to have the same amount of members before looking closer into it
				[[ $IN_THREAD_NUM_MEMBERS -eq ${THREAD_NUM_MEMBERS[$i]} ]] && {
					[[ "$TR_RID" == $(sortwords "${THREAD_RID[$i]}") ]] && {
						TTBL_TID["$T_ID"]=${THREAD_ID[$i]}
						echo -e "Thread matches with ${THREAD_ID[$i]} because source recipient_ids '${T_RID}' match destinations '${THREAD_RID[$i]}'"
						continue 2
						}
					}
			done

# No thread match: copy entry to destination database
		Q_DATE=$(sqlquote "$T_DATE")
		Q_MCOUNT=0 # a trigger will update this on every insert
		Q_RID=$(sqlquote "$TR_RID")
		Q_SNIPPET=$(sqlquote "$T_SNIPPET")
		Q_SNIPPETCS=$(sqlquote "$T_SNIPPETCS")
		Q_READ=$(sqlquote "$T_READ")
		Q_TYPE=$(sqlquote "$T_TYPE")
		Q_ERROR=$(sqlquote "$T_ERROR")
		Q_HASATTACHMENT=$(sqlquote "$T_HASATTACHMENT")
		QUERY="INSERT INTO threads (date, message_count, recipient_ids, \
					    snippet, snippet_cs, read, type, \
					    error, has_attachment) \
		       VALUES ('${Q_DATE}', '${Q_MCOUNT}', '${Q_RID}', \
			       '${Q_SNIPPET}', '${Q_SNIPPETCS}', '${Q_READ}', \
			       '${Q_TYPE}', '${Q_ERROR}', '${Q_HASATTACHMENT}');"
		"$SQLITEBIN" "$OUTDB" "$QUERY"

# Query the destination database for the thread_id of this new entry and add it
# to the lookup table
		QUERY="SELECT _id FROM threads WHERE recipient_ids='${Q_RID}';"
		THREAD_ID["$THREAD_COUNT"]=$("$SQLITEBIN" "$OUTDB" "$QUERY")
		TTBL_TID["$T_ID"]=${THREAD_ID["$THREAD_COUNT"]}
		THREAD_DATE["$THREAD_COUNT"]=$T_DATE
		THREAD_MCOUNT["$THREAD_COUNT"]=$T_MCOUNT # set to zero for starters or keep for reference?
		THREAD_RID["$THREAD_COUNT"]=$T_RID
		THREAD_SNIPPET["$THREAD_COUNT"]=$T_SNIPPET
		THREAD_SNIPPETCS["$THREAD_COUNT"]=$T_SNIPPETCS
		THREAD_READ["$THREAD_COUNT"]=$T_READ
		THREAD_TYPE["$THREAD_COUNT"]=$T_TYPE
		THREAD_ERROR["$THREAD_COUNT"]=$T_ERROR
		THREAD_HASATTACHMENT["$THREAD_COUNT"]=$T_HASATTACHMENT
		THREAD_NUM_MEMBERS["$THREAD_COUNT"]=$IN_THREAD_NUM_MEMBERS
		THREAD_LUT["${THREAD_ID[$THREAD_COUNT]}"]=$THREAD_COUNT
		echo -e "no thread match found, adding to destination: TID: ${THREAD_ID[$THREAD_COUNT]} TDate: ${THREAD_DATE[$THREAD_COUNT]} TRead: ${THREAD_READ[$THREAD_COUNT]} TSnippet: ${THREAD_SNIPPET[$THREAD_COUNT]:0:10} TRID: ${THREAD_RID[$THREAD_COUNT]} TMembers: $(wordcount ${THREAD_RID[$THREAD_COUNT]})"
		((THREAD_COUNT++))
	done < "$INDBFIFO"

# Read and process all messages from source database
QUERY="SELECT thread_id, address, person, date, protocol, read, status, type, \
	      reply_path_present, subject, body, service_center, locked, \
	      error_code, seen \
       FROM sms;"
"$SQLITEBIN" "$INDB" -newline "$LINESEPARATOR" -separator "$COLSEPARATOR" "$QUERY" > "$INDBFIFO" &

while IFS=$COLSEPARATOR read -r -d "$LINESEPARATOR" S_TID S_ADDRESS S_PERSON S_DATE S_PROTOCOL S_READ S_STATUS S_TYPE S_REPLY_PATH_PRESENT S_SUBJECT S_BODY S_SERVICE_CENTER S_LOCKED S_ERROR_CODE S_SEEN
	do
		[[ -z "$S_ADDRESS" ]] && {
			echo "Skipping message with empty Address containing '${S_BODY:0:20}...'"
			continue
		}
		[[ -z "$S_TID" ]] && {
			echo "Skipping message with empty Thread_ID containing '${S_BODY:0:20}...'"
			continue
		}

# Add entry to table sms
		echo "-------------------------------------"
		OUT_TID=${TTBL_TID["$S_TID"]}
		Q_TID=$(sqlquote "$OUT_TID")
		Q_ADDRESS=$(sqlquote "$S_ADDRESS")
		Q_PERSON=$(sqlquote "$S_PERSON")
		Q_DATE=$(sqlquote "$S_DATE")
		Q_PROTOCOL=$(sqlquote "$S_PROTOCOL")
		Q_READ=$(sqlquote "$S_READ")
		Q_STATUS=$(sqlquote "$S_STATUS")
		Q_TYPE=$(sqlquote "$S_TYPE")
		Q_REPLY_PATH_PRESENT=$(sqlquote "$S_REPLY_PATH_PRESENT")
		Q_SUBJECT=$(sqlquote "$S_SUBJECT")
		Q_BODY=$(sqlquote "$S_BODY")
		Q_SERVICE_CENTER=$(sqlquote "$S_SERVICE_CENTER")
		Q_LOCKED=$(sqlquote "$S_LOCKED")
		Q_ERROR_CODE=$(sqlquote "$S_ERROR_CODE")
		Q_SEEN=$(sqlquote "$S_SEEN")
		echo -e "Adding message to destination. TID: $Q_TID Date: $Q_DATE Address: $Q_ADDRESS Subject: '${Q_SUBJECT:0:10}' Body: '${Q_BODY:0:10}'"

		QUERY="INSERT INTO sms (thread_id, address, person, date, \
					protocol, read, status, type, \
					reply_path_present, subject, body, \
					service_center, locked, error_code, \
					seen) \
		       VALUES ('${Q_TID}', '${Q_ADDRESS}', '${Q_PERSON}', \
			       '${Q_DATE}', '${Q_PROTOCOL}', '${Q_READ}', \
			       '${Q_STATUS}', '${Q_TYPE}', \
			       '${Q_REPLY_PATH_PRESENT}', '${Q_SUBJECT}', \
			       '${Q_BODY}', '${Q_SERVICE_CENTER}', \
			       '${Q_LOCKED}', '${Q_ERROR_CODE}', '${Q_SEEN}');"
		"$SQLITEBIN" "$OUTDB" "$QUERY"

		ENTRY=${THREAD_LUT[$OUT_TID]}
# If sms date is lower than thread date, write back current thread entry
		if [[ ${THREAD_DATE["$ENTRY"]} -ge $((S_DATE + DATE_ADJUST)) ]]
			then
				Q_TDATE=$(sqlquote "${THREAD_DATE[$ENTRY]}")
				Q_SNIPPET=$(sqlquote "${THREAD_SNIPPET[$ENTRY]}")
				Q_SNIPPETCS=$(sqlquote "${THREAD_SNIPPETCS[$ENTRY]}")
				Q_READ=$(sqlquote "${THREAD_READ[$ENTRY]}")
				Q_TYPE=$(sqlquote "${THREAD_TYPE[$ENTRY]}")
				Q_ERROR=$(sqlquote "${THREAD_ERROR[$ENTRY]}")
				Q_HASATTACHMENT=$(sqlquote "${THREAD_HASATTACHMENT[$ENTRY]}")
				echo -e "Destination T-Date ${THREAD_DATE[$ENTRY]} newer than adjusted M-Date $((S_DATE + DATE_ADJUST)), writing back. '${Q_SNIPPET:0:10}...'"

				QUERY="UPDATE threads \
				       SET date='${Q_TDATE}', \
					   snippet='${Q_SNIPPET}', \
					   snippet_cs='${Q_SNIPPETCS}', \
					   read='${Q_READ}', type='${Q_TYPE}', \
					   error='${Q_ERROR}', \
					   has_attachment='${Q_HASATTACHMENT}' \
				       WHERE _id='${Q_TID}';"
				"$SQLITEBIN" "$OUTDB" "$QUERY"
			else
# Otherwise overwrite thread date and read in new thread content
				OLD_THREAD_DATE=${THREAD_DATE["$ENTRY"]}
                               QUERY="UPDATE threads \
                                      SET date='${Q_DATE}' \
                                      WHERE _id='${Q_TID}';"
                               "$SQLITEBIN" "$OUTDB" "$QUERY"

				QUERY="SELECT date, message_count, snippet, \
					      snippet_cs, read, type, error, \
					      has_attachment \
				       FROM threads \
				       WHERE _id='${Q_TID}';"
				"$SQLITEBIN" $OUTDB -newline "$LINESEPARATOR" -separator "$COLSEPARATOR" "$QUERY" > "$OUTDBFIFO" &
				COUNT=0

				while IFS=$COLSEPARATOR read -r -d "$LINESEPARATOR" T_DATE T_MCOUNT T_SNIPPET T_SNIPPETCS T_READ T_TYPE T_ERROR T_HASATTACHMENT
					do
						[[ $((COUNT++)) -ge 1 ]] && {
							echo "Error reading thread entry. COUNT should be 1, but is ${COUNT}"
							exit 1
						}
						THREAD_DATE["$ENTRY"]=$T_DATE
						THREAD_MCOUNT["$ENTRY"]=$T_MCOUNT
						THREAD_SNIPPET["$ENTRY"]=$T_SNIPPET
						THREAD_SNIPPETCS["$ENTRY"]=$T_SNIPPETCS
						THREAD_READ["$ENTRY"]=$T_READ
						THREAD_TYPE["$ENTRY"]=$T_TYPE
						THREAD_ERROR["$ENTRY"]=$T_ERROR
						THREAD_HASATTACHMENT["$ENTRY"]=$T_HASATTACHMENT
						echo -e "Destination Thread date $OLD_THREAD_DATE is older than adjusted message date $((S_DATE + DATE_ADJUST)), reading in snippet '${T_SNIPPET:0:10}'"
					done < "$OUTDBFIFO"
			fi
# Next entry of source database
	done < "$INDBFIFO"

echo "Merging done, cleaning up."

# Clean up
cleanup
