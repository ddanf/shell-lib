#!/bin/bash
# This is a library of well-known color sequences as env vars to simultaneously
# promote the twin virtues of colorized shell scripting and code reuse.

# only text foreground colors are currently provided
# the eight main colors of the terminal are hard-coded as you can see:

BOLD="\x1b[1m"
BLACK="\x1b[30m"
RED="\x1b[31m"
GREEN="\x1b[32m"
YELLOW="\x1b[33m"
BLUE="\x1b[34m"
MAGENTA="\x1b[35m"
CYAN="\x1b[36m"
WHITE="\x1b[37m"
RESET="\x1b[0m"
CLRLN="\x1b[2K\r"

# This loop adds the bolded colors as BBLACK, BRED, BGREEN, etc...
# It also adds named functions black(), red(), green(), through white()
#    and the bold versions of the same: bblack(), bred(), bgreen(), etc...
for c in BLACK RED GREEN YELLOW BLUE MAGENTA CYAN WHITE;do
    export "B${c}"=${!c/m/;1m}
    B=B${c}

    f=$(echo ${c} | tr "[:upper:]" "[:lower:]")
    eval "function ${f} () { printf \"${!c}\${*}${RESET}\"; }"

    # printf $(${f} "${f}")
    # printf "\t"

    F=$(echo ${B} | tr "[:upper:]" "[:lower:]")
    eval "function ${F} () { printf \"${!B}\${*}${RESET}\"; }"

done

# This gives the user two choices:
#   1. use the color codes directly in your echo and print statements:
    ${DEBUG} && printf "${BGREEN}This is a test${RESET}\n"
    ${DEBUG} && echo -e ${MAGENTA}This is also a test${RESET}  
#
#   2. call the functions:
    ${DEBUG} && printf "$(bcyan testing)\n"
    ${DEBUG} && echo "$(byellow still testing)"
