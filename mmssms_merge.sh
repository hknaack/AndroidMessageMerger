#!/bin/bash

PROGNAME="$0"
BASENAME=$(basename ${PROGNAME})

# print usage
usage() {
	cat <<-EOF
Usage:
	$BASENAME [options]

	Merge a mmssms.db SQlite message database with an existing one.

Options:
-h		Print this help text.

EOF

	exit 1
}

# check parameters
# print usage if run without options
[[ $# -eq 0 ]] && usage

# parse options
while getopts ":h" option
	do
		case "$option" in
			h|\?) usage
			;;
		esac
	done
shift $(($OPTIND - 1))

# create lookup table from canonical_addresses (destination database) containing
# _id address and stripped down phone numbers in international format
#  (separators removed)


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
