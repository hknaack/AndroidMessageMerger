#!/bin/bash

PROGNAME="$0"
BASENAME=$(basename ${PROGNAME})
OUTDB=
SQLITEBIN="sqlite3"
OUTDBFIFO=/tmp/outdb.fifo
LINESEPARATOR=$'\f'

# print usage
usage() {
	cat <<-EOF
Usage:
	$BASENAME [options]

	Merge a mmssms.db SQlite message database with an existing one.

Options:
-h		Print this help text.
-o file.db	Filename of SQlite database, into which messages should be
		merged. (required option!)

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

outfile() {
	filecheck "$1" "w" || {
		echo_err "Error: Output file could not be opened!"
		exit 1
	}
	OUTDB="$1"

	return 0
}

# check parameters
# print usage if run without options
[[ $# -eq 0 ]] && usage

# parse options
while getopts ":ho:" option
	do
		case "$option" in
			h|\?) usage
			;;
			o) outfile "$OPTARG"
			;;
		esac
	done
shift $(($OPTIND - 1))

[[ -z $OUTDB ]] && usage

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
		((LUT_COUNT++))
	done < "$OUTDBFIFO"

# Dump content of lookup table for debugging
for ((i = 0; i < LUT_COUNT; i++))
	do
		echo -e "Lookup-Table ID: ${LUT_ID[$i]} \t Address: ${LUT_ADDRESS[$i]}"
	done

# query source database, cycle through entries

# strip down address


# compare address with lookup table


# if not existing, add entry to table canonical_addresses in destination
# database
# "INSERT INTO canonical_addresses (address) VALUES (\"${address.Source}\");"

# get _id of that entry
# "SELECT DISTINCT _id FROM canonical_addresses WHERE address=\"${address.Source}\";"

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
rm "$OUTDBFIFO"
