#!/bin/bash

PROGNAME="$0"
BASENAME=$(basename ${PROGNAME})
INDB=
OUTDB=
SQLITEBIN="sqlite3"
INDBFIFO=/tmp/indb.fifo
OUTDBFIFO=/tmp/outdb.fifo
LINESEPARATOR=$'\f'
PREFIX_PATTERN=
PREFIX_REPLACE=

# print usage
usage() {
	cat <<EOF
Usage:
	$BASENAME [options]

	Merge a mmssms.db SQlite message database with an existing one.

Options:
-h		Print this help text.
-i file.db	Filename of input SQlite database, where messages should be read
		from. (required option!)
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
echo_err () {
	echo "$@" >&2
}

filecheck() {
	[[ ! -f "$1" && ! -r "$1" ]] && return 1
	[[ "$2" == "w" && ! -w "$1" ]] && {
		echo_err "Error: File $1 is not writable!"
		return 1
	}

	return 0
}

infile() {
	filecheck "$1" "r" || {
		echo_err "Error: Input file could not be opened!"
		exit 1
	}
	INDB="$1"

	return 0
}

outfile() {
	filecheck "$1" "w" || {
		echo_err "Error: Output file could not be opened!"
		exit 1
	}
	OUTDB="$1"

	return 0
}

# strip out separators
stripped () {
	local strip="$1"
	strip=${strip//" "}
	strip=${strip//"-"}
	strip=${strip//"/"}
	strip=${strip//"("}
	strip=${strip//")"}
	echo "$strip"
}

# add international prefix
international_prefix () {
	local number="$1"
	[[ ${number:0:2} == '00' ]] && number="${number/00/+}"
	[[ -n $PREFIX_PATTERN && -n $PREFIX_REPLACE ]] && {
		local LEN=${#PREFIX_PATTERN}
		[[ ${number:0:${LEN}} == "$PREFIX_PATTERN" ]] &&
			number="${number/$PREFIX_PATTERN/$PREFIX_REPLACE}"
	}
	echo "$number"
}

# convert phone number to international format, if it starts with 00
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

# check if a given string is a phone number in international format
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

# minimum function, return the smaller of two values
min () {
	echo $(( $1 < $2 ? $1 : $2 ))
}

# check if two given address strings match
matchaddr () {
	local FIRST="$1"
	local SECOND="$2"
	local EXACT="$3"

	if [[ $EXACT == 'true' ]]
		then
			[[ "$FIRST" == "$SECOND" ]] && {
				echo 'true'
				return 0
			}
		else
			local FIRST_LEN=${#FIRST}
			local SECOND_LEN=${#SECOND}
			local DELTA
			(( DELTA = FIRST_LEN - SECOND_LEN))
			[[ ${DELTA##-} -ge 3 ]] && {
				echo 'false'
				return 1
			}
			local MIN=$( min "$FIRST_LEN" "$SECOND_LEN" )
			[[ $MIN -gt 6 ]] && (( MIN -= 3))
			[[ "${FIRST:(-$MIN)}" == "${SECOND:(-$MIN)}" ]] && {
				echo 'true'
				return 0
			}
	fi

	echo 'false'
	return 1
}

# replace single quotes (') by two single quotes ('') for SQL content
sqlquote () {
	echo "${1//\'/\'\'}"
}

# check parameters
# print usage if run without options
[[ $# -eq 0 ]] && usage

# parse options
while getopts ":hi:o:p:r:" option
	do
		case "$option" in
			h|\?) usage
			;;
			i) infile "$OPTARG"
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

# create lookup table from canonical_addresses (destination database) containing
# _id address and stripped down phone numbers in international format
# (separators removed)
[[ -e "$OUTDBFIFO" ]] || mkfifo "$OUTDBFIFO"

LUT_COUNT=0
QUERY="SELECT _id, address FROM canonical_addresses ORDER BY _id;"
"$SQLITEBIN" "$OUTDB" -newline "$LINESEPARATOR" "$QUERY" > "$OUTDBFIFO" &

while IFS='|' read -r -d "$LINESEPARATOR" CANONICALID CANONICALADDRESS
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

# query source database, cycle through entries
[[ -e "$INDBFIFO" ]] || mkfifo "$INDBFIFO"

QUERY="SELECT _id, address FROM canonical_addresses ORDER BY _id;"
"$SQLITEBIN" "$INDB" -newline "$LINESEPARATOR" "$QUERY" > "$INDBFIFO" &

while IFS='|' read -r -d "$LINESEPARATOR" CANONICALID CANONICALADDRESS
	do
# convert CANONICALADDRESS to international format
		CAN_ADDR_STRIPPED=$(internationalized $(stripped "$CANONICALADDRESS"))
		INT_NUM=$(internationalnumber "$CAN_ADDR_STRIPPED")

# search through LUT_ADDRESS_STRIPPED for this address
		for ((i = 0; i < LUT_COUNT; i++))
			do
				RET=$(matchaddr "$CAN_ADDR_STRIPPED" "${LUT_ADDRESS_STRIPPED[$i]}" "$INT_NUM")
# on match, add entry to translation table, continue with next entry
				[[ "$RET" == 'true' ]] && {
					TTBL_ID["$CANONICALID"]=${LUT_ID[$i]}
					echo "_id $CANONICALID ($CANONICALADDRESS) from infile matches _id ${LUT_ID[$i]} (${LUT_ADDRESS[$i]}) from outfile"
					continue 2
				}
			done
# no match: add address (in international format, if it is a phone number) to
# destination database
		echo "_id $CANONICALID ($CANONICALADDRESS) did not match any entry in outfile"
		if [[ "$INT_NUM" == 'true' ]]
			then
				NEW_ADDR=$(sqlquote "$CAN_ADDR_STRIPPED")
			else
				NEW_ADDR=$(sqlquote "$CANONICALADDRESS")
		fi

		QUERY="INSERT INTO canonical_addresses (address) VALUES ('${NEW_ADDR}');"
		"$SQLITEBIN" "$OUTDB" "$QUERY"
# query the destination database for the _id of this new entry and add a new
# entry to the lookup table
		QUERY="SELECT _id FROM canonical_addresses WHERE address='${NEW_ADDR}';"
		LUT_ID["$LUT_COUNT"]=$("$SQLITEBIN" "$OUTDB" "$QUERY")
		LUT_ADDRESS["$LUT_COUNT"]=$NEW_ADDR
		LUT_ADDRESS_STRIPPED["$LUT_COUNT"]=$CAN_ADDR_STRIPPED
		TTBL_ID["$CANONICALID"]=${LUT_ID[$LUT_COUNT]}
		echo "address $NEW_ADDR was added to outfile, its new _id is ${LUT_ID[$LUT_COUNT]}"
		((LUT_COUNT++))
done < "$INDBFIFO"

# get a local copy of table threads of destination database
THREAD_COUNT=0
QUERY="SELECT _id, date, message_count, recipient_ids, snippet, snippet_cs, \
	      read, type, error, has_attachment \
       FROM threads \
       ORDER BY _id ASC;"
"$SQLITEBIN" "$OUTDB" -newline "$LINESEPARATOR" "$QUERY" > "$OUTDBFIFO" &

while IFS='|' read -r -d "$LINESEPARATOR" T_ID T_DATE T_MCOUNT T_RID T_SNIPPET T_SNIPPETCS T_READ T_TYPE T_ERROR T_HASATTACHMENT
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
		((THREAD_COUNT++))
	done < "$OUTDBFIFO"

# dump content of threads table for debugging
for ((i = 0; i < THREAD_COUNT; i++))
	do
		echo -e "TID: ${THREAD_ID[$i]} TDate: ${THREAD_DATE[$i]} TRead: ${THREAD_READ[$i]} TSnippet: ${THREAD_SNIPPET[$i]:0:10} TRID: ${THREAD_RID[$i]}"
	done

# add entry to table threads in destination database
# "INSERT INTO threads (recipient_ids) VALUES (?)", _id


# update table threads
# "UPDATE threads SET message_count=message_count + 1,snippet=?,'date'=? WHERE recipient_ids=? ", body.Source, date.Source, _id]

# get thread_id of that entry
# "SELECT _id FROM threads WHERE recipient_ids=? ", _id

# add entry to table sms
# "INSERT INTO sms (address,'date',body,thread_id,read,type,seen) VALUES (?,?,?,?,1,?,1)", address.Source,date.Source,body.Source,Thread_id,type.Source


# next entry of source database

# clean up
rm "$INDBFIFO" "$OUTDBFIFO"
