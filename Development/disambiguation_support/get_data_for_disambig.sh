#!/bin/bash
CONF_FILE="$1"
OUTPUT_LOCATION="$2"

echo "$CONF_FILE"
echo "$OUTPUT_LOCATION"

mkdir -p $OUTPUT_LOCATION

for table in rawinventor patent cpc_current ipcr nber rawassignee uspc_current rawlawyer rawlocation 
do
    if [ -e "$OUTPUT_LOCATION/$table.tsv" ]
    then
	continue
    fi
    echo "$table"
    # Set the SELECT statement that will create the table
    case "$table" in 
        # Tables with custom select statements
        ("rawinventor") selectStatement="SELECT uuid, patent_id, inventor_id, rawlocation_id, name_first, name_last, sequence from rawinventor" ;;
        ("patent") selectStatement="SELECT id, type, number, country, date, abstract, title, kind, num_claims, filename from patent" ;;
        ("rawlocation") selectStatement="SELECT id,location_id_transformed as location_id,city,state,country_transformed as country from rawlocation" ;;
        # Tables with standard select statements
        (*) selectStatement="SELECT * FROM $table"
    esac

    echo "$selectStatement" | mysql --defaults-file="$CONF_FILE" >"$OUTPUT_LOCATION/$table.tsv"

done


