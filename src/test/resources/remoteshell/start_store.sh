#!/bin/bash:  
function usage(){
        echo -e "Invalid argument count, please provide the host ip for starting (multiple ip separated by commas) and script target directory"
}

case $# in
2)
        stop_client_serial=${1}
        OLD_IFS="$IFS"
        IFS=","
        # shellcheck disable=SC2206
        clientarr=($stop_client_serial)
        IFS="$OLD_IFS"
        HA_PATH=${2}
        # shellcheck disable=SC2164
        cd ${HA_PATH}
        # shellcheck disable=SC2164
        cd ../dingo-deploy
    
        # shellcheck disable=SC2068
        for cli in ${clientarr[@]}
        do
                ./DingoControl start one_store $cli
        done
        ;;
*)
        usage
        exit 1
esac