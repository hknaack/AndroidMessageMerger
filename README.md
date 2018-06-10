# AndroidMessageMerger
This script helps to merge message threads, containing SMS and MMS, from an
existing Android mmssms.db database into another such database.

# Tested Android versions:
Android 2.3 (source database)
Android 4.2.2 (source and destination database)

# Required software:
Bash (tested on 4.3.30)
SQlite (version 3.8.8 or newer)

# Recommended procedure:
1. Backup your phone data (preferably Nandroid)!
2. Restart your phone and make sure it will not connect to your phone network
   (put into airplane mode or remove SIM card).
3. Copy your phones mmssms.db file (for example with adb pull from
   /data/data/com.android.providers.telephony/databases/).
4. Do a dry run of this script to check if it recognizes your phone numbers
   correctly. Use the -p and -r options to adjust the country prefix.
5. Run the script to merge your message threads from the source database into
   the destination database.
6. Rename the mmssms.db file on your phone (mv mmssms.db mmssms.db.bak).
7. Copy the merged mmssms.db file back to your phone (for example with
   adb push).
8. Adjust owner (chown radio:radio mmssms.db) and permission
   (chmod 660 mmssms.db).
9. Reboot your phone.
10.Start message app

# Usage:
	mmssms_merge.sh -i mmssms_in.db -o mmssms_out.db [options]

Options | Description
---|---
-a | Adjust tolerance time between messages and their threads in ms (default value: 3000)
-B | Disable automatic backup of destination database (not recommended)
-c string | Coloumn separator used for SQlite database queries (should not be used in the database entries, default value: '\|')
-d | Dry run. Just list the found phone numbers and its matches, then exit.
-f | Directory (with write permission) to use for temporary FIFO files (defaults to /tmp)
-h | Print help text.
-i file.db | Filename of input SQlite database, where messages should be read from. (required option!)
-l string | Line separator used for SQlite database queries (should not be used in the database entries, default value: '\f')
-o file.db | Filename of SQlite database, into which messages should be merged. (required option!)
-p string | National phone number prefix to be replaced with -r (only works together with -r)
-r string | International prefix to replace the code given with -p (only works together with -p)
