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
DATE_ADJUST=3000

# print usage
usage() {
	cat <<EOF
Usage:
	$BASENAME [options]

	Merge a mmssms.db SQlite message database with an existing one.

Options:
-a		Adjust tolerance time between messages and their threads in ms
		(default value: 3000)
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

set_dateadjust () {
	local number="$1"
	case $number in
		''|*[!0-9]*) usage
		;;
		*) DATE_ADJUST=$number
		;;
	esac
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

# count words in a string
wordcount () {
	echo $(echo "$1" | wc -w)
}

# sort words in a string
sortwords () {
	local sorted=$(
		for el in $1
			do
				echo "$el"
			done | sort -n )
	echo "${sorted[@]}"
}

# translate canonical ids
translate_cids () {
	local MEMBER
	local TRANS_MEMBER=$(
		for MEMBER in $1
			do
				echo "${TTBL_ID[$MEMBER]}"
			done)
	echo "${TRANS_MEMBER[@]}"
}

# check parameters
# print usage if run without options
[[ $# -eq 0 ]] && usage

# parse options
while getopts ":a:hi:o:p:r:" option
	do
		case "$option" in
			a) set_dateadjust "$OPTARG"
			;;
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
		THREAD_NUM_MEMBERS["$THREAD_COUNT"]=$(wordcount "$T_RID")
		THREAD_LUT["$T_ID"]="$THREAD_COUNT"
		((THREAD_COUNT++))
	done < "$OUTDBFIFO"

# dump content of threads table for debugging
for ((i = 0; i < THREAD_COUNT; i++))
	do
		echo -e "TID: ${THREAD_ID[$i]} TDate: ${THREAD_DATE[$i]} TRead: ${THREAD_READ[$i]} TSnippet: ${THREAD_SNIPPET[$i]:0:10} TRID: ${THREAD_RID[$i]} TMembers: $(wordcount ${THREAD_RID[$i]})"
	done

# compare entries of table threads of source database and add them to
# destination database if needed
echo "Processing threads from source database"
QUERY="SELECT _id, date, message_count, recipient_ids, snippet, snippet_cs, \
	      read, type, error, has_attachment \
       FROM threads \
       ORDER BY _id ASC;"
"$SQLITEBIN" "$INDB" -newline "$LINESEPARATOR" "$QUERY" > "$INDBFIFO" &

while IFS='|' read -r -d "$LINESEPARATOR" T_ID T_DATE T_MCOUNT T_RID T_SNIPPET T_SNIPPETCS T_READ T_TYPE T_ERROR T_HASATTACHMENT
	do
		echo "-------------------------"
		IN_THREAD_NUM_MEMBERS=$(wordcount "$T_RID")
		echo -e "TID: $T_ID TDate: $T_DATE TRead: $T_READ TSnippet: ${T_SNIPPET:0:10} TRID: $T_RID TMembers: $IN_THREAD_NUM_MEMBERS"
		TR_RID=$(translate_cids "$T_RID")
		TR_RID=$(sortwords "$TR_RID")
		for ((i = 0; i < THREAD_COUNT; i++))
			do
# make sure to have the same amount of members before looking closer into it
				[[ $IN_THREAD_NUM_MEMBERS -eq ${THREAD_NUM_MEMBERS[$i]} ]] && {
					[[ "$TR_RID" == $(sortwords "${THREAD_RID[$i]}") ]] && {
						TTBL_TID["$T_ID"]=${THREAD_ID[$i]}
						echo -e "Thread matches with ${THREAD_ID[$i]} because source recipient_ids '${T_RID}' match destinations '${THREAD_RID[$i]}'"
						continue 2
						}
					}
			done

# no thread match: copy entry to destination database
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

# query the destination database for the thread_id of this new entry and add it
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

# read and process all messages from source database
QUERY="SELECT thread_id, address, person, date, protocol, read, status, type, \
	      reply_path_present, subject, body, service_center, locked, \
	      error_code, seen \
       FROM sms;"
"$SQLITEBIN" "$INDB" -newline "$LINESEPARATOR" "$QUERY" > "$INDBFIFO" &

while IFS='|' read -r -d "$LINESEPARATOR" S_TID S_ADDRESS S_PERSON S_DATE S_PROTOCOL S_READ S_STATUS S_TYPE S_REPLY_PATH_PRESENT S_SUBJECT S_BODY S_SERVICE_CENTER S_LOCKED S_ERROR_CODE S_SEEN
	do
		[[ -z "$S_ADDRESS" ]] && {
			echo "Skipping message with empty Address containing '${S_BODY:0:20}...'"
			continue
		}
		[[ -z "$S_TID" ]] && {
			echo "Skipping message with empty Thread_ID containing '${S_BODY:0:20}...'"
			continue
		}

# add entry to table sms
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
# if sms date is lower than thread date, write back current thread entry
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
# otherwise overwrite thread date and read in new thread content
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
				"$SQLITEBIN" $OUTDB -newline "$LINESEPARATOR" "$QUERY" > "$OUTDBFIFO" &
				COUNT=0

				while IFS='|' read -r -d "$LINESEPARATOR" T_DATE T_MCOUNT T_SNIPPET T_SNIPPETCS T_READ T_TYPE T_ERROR T_HASATTACHMENT
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
# next entry of source database
	done < "$INDBFIFO"

echo "Merging done, cleaning up."

# clean up
rm "$INDBFIFO" "$OUTDBFIFO"
