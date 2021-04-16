#!/bin/bash
#
# submitLivyJob.sh
# submits a job to the Livy API and then polls until the job is completed, reporting
# status back to stdout
#

SCRIPT_DIR=$(dirname ${0})
. ${SCRIPT_DIR}/livyConfig
DEBUG=false . ${SCRIPT_DIR}/shColors.sh

################################################################################
# Functions
################################################################################
function usage {
    # simple usage function for when we get unexpected input
    cat <<EOF
    submitLivyJob.sh -- a script to submit a batch job request via the Livy API
                        and to poll the Livy service for the batch status until
                        it is complted.

        *** BEFORE RUNNING *** run kinit to get a kerberos ticket.  otherwise
                        the script will fail with useless error messages
    
        Usage: submitLivyJob.sh -b <sparkBatchID> [-e <livyURL>] [-j <batchJar>] \
                        [-a <batch class>] [-S <max results size>] \
                        [-p <driver extra classpath>] [-P <executor extra classpath>] \
                        [-I <AWS Access Key ID>] [-K <AWS Secret Key>] \
                        [-m <driver memory>] [-c <driver cores>] \
                        [-M <executor memory>] [-C <executor cores>] \
                        [-t <keytab name>] [-k <kerberos principal> ] \
                        [-l <sleep time>] [-X <max request time>] [-r] | [-h|?] 
       
        -t specifies a key table to use
        -k is the kerberos principal (user@domain)
        -l specifies how long to sleep between polls for batch status
        -X specifies how long curl should wait for connection and response (combined)
        -r tells the script to output the response JSON
EOF
}

function getIndex () {
    # search for the first arg in the list of remaining args and give the number at which it occurs within that list
    local e match="$1"
    shift
    for e; do 
        if [[ "$e" == "$match" ]];then
            echo ${index} 
            return 0
        fi
        ((index++))
    done
    echo -1
    return 0
    }

function stateColor () {
    # add a little color to our output
    case "${1}" in
        not_started|starting)
            echo ${BLUE}
            ;;
        shutting_down)
            echo ${YELLOW}
            ;;
        error|dead|killed)
            echo ${RED}
            ;;
        idle|busy|success)
            echo ${GREEN}
            ;;
        *)
            echo ${RESET}
            ;;
    esac
    }

function submitJob () {
    # this curl command submits the job and returns the JSON output to $RES
    RES=$(curl -XPOST --silent --negotiate -u : --max-time ${MAX_TIME} --fail ${LIVY_URL} -d "{ \
        \"className\": \"${INGEST_CLASS}\", \
        \"args\": [\"--batchId=${LIVY_BATCH}\"], \
        \"file\": \"${INGEST_JAR}\", \
        \"files\": [\"${INGEST_CONF}\"], \
        \"jars\": [\"${DRIVER_CLASS}\"], \
        \"conf\": { \
            \"spark.driver.maxResultSize\": \"${MAX_RES_SIZE}\", \
            \"spark.driver.extraClassPath\":\"${DRIVER_CLASS}\", \
            \"spark.executor.extraClassPath\":\"${EXEC_CLASS}\", \
            \"spark.yarn.queue\":\"${YARN_POOL_NAME}\", \
            \"spark.acls.enable\":\"true\", \
            \"spark.ui.view.acls\":\"${SPARK_UI_USERS}\", \
            \"spark.ui.view.acls.groups\":\"${SPARK_UI_USER_GROUPS}\", \
            \"spark.history.ui.acls.enable\":\"true\", \
            \"spark.history.ui.admin.acls\":\"${SPARK_UI_USERS}\", \
            \"spark.history.ui.admin.acls.groups\":\"${SPARK_UI_USER_GROUPS}\", \
            \"spark.dynamicAllocation.maxExecutors\":${EXEC_CNT_MAX}  \
       }, \
        \"numExecutors\": ${EXEC_CNT_INITIAL}, \
        \"executorMemory\": \"${EXEC_MEM}\", \
        \"executorCores\": ${EXEC_CORES}, \
        \"driverMemory\": \"${DRIVER_MEM}\", \
        \"driverCores\": ${DRIVER_CORES} \
    }" -H 'Content-Type: application/json')
    rc=$?
    echo ${RES}
    return ${rc}

    # For now we rely on keys being set up in the spark environment, but
    #   if we need to send them for some reason, the following lines would
    #   need to be uncommented and then reinserted into the curl command
            # \"spark.hadoop.fs.s3a.awsAccessKeyId\": \"${AWS_KEY_ID}\", \
            # \"spark.hadoop.fs.s3a.awsSecretAccessKey\": \"${AWS_KEY}\" \
    }

function getBatch () {
    # interface to the batch status API
    curl --silent --negotiate -u : --max-time ${MAX_TIME} --fail "${LIVY_URL}/${1}"
    rc=$?
    return ${rc}
    }

function retry () {
    # this wrapper takes a number of retries as its first argument and then
    # executes the remaining list of args as a command that number of times
    # or until those commands produce a return code of 0.
    RETRIES=${1}
    shift
    k=0
    rc=1
    while [ ${k} -lt ${RETRIES} -a ${rc} -ne 0 ]; do 
        ${*}
        rc=$?
        ((k++))
    done
    return ${rc}
    }

################################################################################
# MAIN
################################################################################
RESPONSE_OUT=false
BLOCKING_CALL=false

# process command line args
MYOPTS="e:b:j:S:p:P:I:K:M:C:m:c:t:k:l:X:rBh?"
while getopts "$MYOPTS" o; do
    case "${o}" in
        e)
            LIVY_URL=${OPTARG}
            ;;
        b)
            LIVY_BATCH=${OPTARG}
            ;; 
        j)
            INGEST_JAR=${OPTARG}
            ;;
        a)
            INGEST_CLASS=${OPTARG}
            ;;
        S)
            MAX_RES_SIZE=${OPTARG}
            ;;
        p)
            DRIVER_CLASS=${OPTARG}
            ;;
        P)
            EXEC_CLASS=${OPTARG}
            ;;
        I)
            AWS_KEY_ID=${OPTARG}
            ;;
        K)
            AWS_KEY=${OPTARG}
            ;;
        M)
            EXEC_MEM=${OPTARG}
            ;;
        C)
            EXEC_CORES=${OPTARG}
            ;;
        m)
            DRIVER_MEM=${OPTARG}
            ;;
        c)
            DRIVER_CORES=${OPTARG}
            ;;
        t)
            KEYTAB_NAME=${OPTARG}
            ;;
        k)
            KERB_PRINCIPAL=${OPTARG}
            ;;
        l)
            SLEEP_TIME=${OPTARG}
            ;;
        X)
            MAX_TIME=${OPTARG}
            ;;
        r)
            RESPONSE_OUT=true
            ;;
        B)
            BLOCKING_CALL=true
            ;;
        *)
            usage
            exit
            ;;
    esac
done
shift $((OPTIND-1))

# set defaults for required items that were not provided in config or command line
MAX_TIME=${MAX_TIME:=30}
SLEEP_TIME=${SLEEP_TIME:=10}
DEBUG=${DEBUG:=false}

[ -z ${KEYTAB_NAME} ]  || KEYTAB_NAME="-t ${KEYTAB_NAME}"

# check for errors
[ -n "${LIVY_URL}" ]   || ERRMSG="Livy environment must be selected with -e switch.\n"
[ -n "${INGEST_JAR}" ] || ERRMSG="${ERRMSG}  Ingest job .jar filename must be specified on command line with -j or in the livyConfig file.\n"
[ -n "${LIVY_BATCH}" ] || ERRMSG="${ERRMSG}  Ingest batch id must be specified on command line with -b on in the livyConfig file.\n"

# renew the ticket if needed
if ! klist -s; then
    echo "\ndoing kinit to renew ticket KERB_PRINCIPAL=${KERB_PRINCIPAL} and KEYTAB_NAME=${KEYTAB_NAME}"
    retry ${KERB_TRIES} kinit ${KERB_PRINCIPAL} ${KEYTAB_NAME}
    rc=$?
    [ ${rc} -eq 0 ] && echo -e "${BGREEN}kerberos ticket acquired${RESET}\n" || ERRMSG="${ERRMSG}  ${BRED}Failed to obtain a kerberos ticket.  Please check principal/keytab settings.${RESET}\n"
else
    echo "\nValid Kerberos ticket already exists"
fi

# check for any errors; print any out and then exit
[ -z "${ERRMSG}" ] || { echo -e ${ERRMSG}; exit 1; }

# one last validation that we have a valid kerberos ticket and then submit the job via retry
klist -s || { echo -e "${BOLD}${RED}no kerberos ticket${RESET}"; exit 1; }
echo "submitting livy/scala job"
RES=$(retry ${CURL_TRIES} submitJob)

# dump the response if DEBUG
${DEBUG} && { echo "--------------------------------------------------------------------------------";
    echo "job submittal response:";
    echo "${RES}";
    echo "--------------------------------------------------------------------------------"; }

# get the time and pull the batch number from the response
SUB_TIME=$(date +%s)
BNUM=$(echo ${RES} | jq '.id') 

if [[ -z "${BNUM}" ]]; then
    echo "Livy Response = ${RES}"
    echo "Livy BatchNumber not returned. Exiting the Job."
    exit 1
fi

printf "${CLRLN}${BOLD}checking batch ${BNUM} status${RESET}\n" 

# make an initial attempt to pull the batch status
BRES=$(retry ${CURL_TRIES} getBatch ${BNUM})

# dump the response if DEBUG
${DEBUG} && { echo "--------------------------------------------------------------------------------";
    echo "  batch inquiry response:";
    echo "${BRES}";
    echo "--------------------------------------------------------------------------------"; }

# pull the state and turn the state into an index into the list of state messages
BSTATE=$(echo ${BRES} | jq '.state' | tr -d '"')
IDX=$(getIndex $BSTATE ${stateKey[@]})

# get the color for the specified state and then print it
ST_COLOR=$(stateColor ${BSTATE})
printf "${CLRLN}  ${BOLD}(0 seconds) status=${ST_COLOR}${BSTATE}${RESET}"

# if it's not a blocking call then return if the status is greater than starting
if [ "$BLOCKING_CALL" = false ] ; then
    FAIL_STATE=3
    echo "This is a non-blocking call, will return as soon as the Livy Job has started..."
fi

# poll the state until the job is ended
until [[ ${IDX} -ge ${FAIL_STATE} ]];do 
    BRES=$(retry ${CURL_TRIES} getBatch ${BNUM})
    STAT_TIME=$(date +%s)
    JOB_TIME=$((${STAT_TIME}-${SUB_TIME}))
    BSTATE=$(echo ${BRES} | jq '.state' | tr -d '"')
    IDX=$(getIndex ${BSTATE} ${stateKey[@]})
    ST_COLOR=$(stateColor ${BSTATE})
    printf "${CLRLN}  ${BOLD}(${JOB_TIME} seconds) status=${ST_COLOR}${BSTATE}${RESET}"
    [[ ${IDX} -lt ${FAIL_STATE} ]] && sleep ${SLEEP_TIME}
done

# final output
echo
echo "================================================================================"
printf "${CLRLN}Batch #${BNUM} completed with status=${RESET}${ST_COLOR}${BSTATE}${RESET}\n"
echo "================================================================================"
${RESPONSE_OUT} && { echo -e "${RESET}Batch Response JSON:"; echo ${BRES} | jq; }
