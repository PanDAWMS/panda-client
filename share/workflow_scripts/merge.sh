#!/bin/bash
# Arguments:
#   $1  Output filename for the merged ROOT file (default: merge.root)
#   $2  Input file containing the list of files to merge, written by prun via --writeInputToTxt (default: input.lis)

OUTPUT=${1:-merge.root}
INPUT_FILE=${2:-input.lis}
sed -i 's/,/\n/g' "$INPUT_FILE"
hadd -f "$OUTPUT" "@$INPUT_FILE"
