#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

bin=$(dirname "${BASH_SOURCE-$0}")
bin=$(cd "${bin}">/dev/null; pwd)

function usage() {
    echo "usage) $0 -h <host> -p <port> -d <interpreter dir to load> -l <local interpreter repo dir to load>"
}

while getopts "h:m:p:d:l:v" o; do
    case ${o} in
        m)
            MASTER=${OPTARG}
            ;;
        d)
            INTERPRETER_DIR=${OPTARG}
            ;;
        h)
            HOST=${OPTARG}
            ;;
        p)
            PORT=${OPTARG}
            ;;
        l)
            LOCAL_INTERPRETER_REPO=${OPTARG}
            ;;
        v)
            . "${bin}/common.sh"
            getZeppelinVersion
            ;;
        esac
done


if [ -z "${HOST}" ] || [ -z "${PORT}" ] || [ -z "${INTERPRETER_DIR}" ]; then
    usage
    exit 1
fi

. "${bin}/common.sh"

ZEPPELIN_INTP_CLASSPATH=""

# construct classpath
if [[ -d "${ZEPPELIN_HOME}/zeppelin-interpreter/target/classes" ]]; then
  ZEPPELIN_INTP_CLASSPATH+=":${ZEPPELIN_HOME}/zeppelin-interpreter/target/classes"
else
  ZEPPELIN_INTERPRETER_JAR="$(ls ${ZEPPELIN_HOME}/lib/zeppelin-interpreter*.jar)"
  ZEPPELIN_INTP_CLASSPATH+=":${ZEPPELIN_INTERPRETER_JAR}"
fi

# add test classes for unittest
if [[ -d "${ZEPPELIN_HOME}/zeppelin-interpreter/target/test-classes" ]]; then
  ZEPPELIN_INTP_CLASSPATH+=":${ZEPPELIN_HOME}/zeppelin-interpreter/target/test-classes"
fi
if [[ -d "${ZEPPELIN_HOME}/zeppelin-zengine/target/test-classes" ]]; then
  ZEPPELIN_INTP_CLASSPATH+=":${ZEPPELIN_HOME}/zeppelin-zengine/target/test-classes"
fi


addJarInDirForIntp "${ZEPPELIN_HOME}/zeppelin-interpreter/target/lib"
addJarInDirForIntp "${INTERPRETER_DIR}"

HOSTNAME=$(hostname)
ZEPPELIN_SERVER=org.apache.zeppelin.interpreter.remote.RemoteInterpreterServer

INTERPRETER_ID=$(basename "${INTERPRETER_DIR}")
ZEPPELIN_PID="${ZEPPELIN_PID_DIR}/zeppelin-interpreter-${INTERPRETER_ID}-${ZEPPELIN_IDENT_STRING}-${HOSTNAME}.pid"
ZEPPELIN_LOGFILE="${ZEPPELIN_LOG_DIR}/zeppelin-interpreter-${INTERPRETER_ID}-${ZEPPELIN_IDENT_STRING}-${HOSTNAME}.log"
JAVA_INTP_OPTS+=" -Dzeppelin.log.file=${ZEPPELIN_LOGFILE}"

if [[ ! -d "${ZEPPELIN_LOG_DIR}" ]]; then
  echo "Log dir doesn't exist, create ${ZEPPELIN_LOG_DIR}"
  $(mkdir -p "${ZEPPELIN_LOG_DIR}")
fi

# set spark related env variables
if [[ "${INTERPRETER_ID}" == "spark" ]]; then
  if [[ -n "${SPARK_HOME}" ]]; then
    export SPARK_SUBMIT="${SPARK_HOME}/bin/spark-submit"
    SPARK_APP_JAR="$(ls ${ZEPPELIN_HOME}/interpreter/spark/zeppelin-spark*.jar)"
    # This will evantually passes SPARK_APP_JAR to classpath of SparkIMain
    ZEPPELIN_INTP_CLASSPATH+=":${SPARK_APP_JAR}"

    pattern="$SPARK_HOME/python/lib/py4j-*-src.zip"
    py4j=($pattern)
    # pick the first match py4j zip - there should only be one
    export PYTHONPATH="$SPARK_HOME/python/:$PYTHONPATH"
    export PYTHONPATH="${py4j[0]}:$PYTHONPATH"
  else
    # add Hadoop jars into classpath
    if [[ -n "${HADOOP_HOME}" ]]; then
      # Apache
      addEachJarInDirRecursiveForIntp "${HADOOP_HOME}/share"

      # CDH
      addJarInDirForIntp "${HADOOP_HOME}"
      addJarInDirForIntp "${HADOOP_HOME}/lib"
    fi

    addJarInDirForIntp "${INTERPRETER_DIR}/dep"

    pattern="${ZEPPELIN_HOME}/interpreter/spark/pyspark/py4j-*-src.zip"
    py4j=($pattern)
    # pick the first match py4j zip - there should only be one
    PYSPARKPATH="${ZEPPELIN_HOME}/interpreter/spark/pyspark/pyspark.zip:${py4j[0]}"

    if [[ -z "${PYTHONPATH}" ]]; then
      export PYTHONPATH="${PYSPARKPATH}"
    else
      export PYTHONPATH="${PYTHONPATH}:${PYSPARKPATH}"
    fi
    unset PYSPARKPATH

    # autodetect HADOOP_CONF_HOME by heuristic
    if [[ -n "${HADOOP_HOME}" ]] && [[ -z "${HADOOP_CONF_DIR}" ]]; then
      if [[ -d "${HADOOP_HOME}/etc/hadoop" ]]; then
        export HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"
      elif [[ -d "/etc/hadoop/conf" ]]; then
        export HADOOP_CONF_DIR="/etc/hadoop/conf"
      fi
    fi

    if [[ -n "${HADOOP_CONF_DIR}" ]] && [[ -d "${HADOOP_CONF_DIR}" ]]; then
      ZEPPELIN_INTP_CLASSPATH+=":${HADOOP_CONF_DIR}"
    fi

    export SPARK_CLASSPATH+=":${ZEPPELIN_INTP_CLASSPATH}"
  fi
elif [[ "${INTERPRETER_ID}" == "hbase" ]]; then
  if [[ -n "${HBASE_CONF_DIR}" ]]; then
    ZEPPELIN_INTP_CLASSPATH+=":${HBASE_CONF_DIR}"
  elif [[ -n "${HBASE_HOME}" ]]; then
    ZEPPELIN_INTP_CLASSPATH+=":${HBASE_HOME}/conf"
  else
    echo "HBASE_HOME and HBASE_CONF_DIR are not set, configuration might not be loaded"
  fi
fi

addJarInDirForIntp "${LOCAL_INTERPRETER_REPO}"

CLASSPATH+=":${ZEPPELIN_INTP_CLASSPATH}"

if [[ -n "${SPARK_SUBMIT}" ]]; then
    if [[ "${MASTER}" == "yarn-cluster" ]] || [[ "${MASTER}" == "cluster" ]]; then
        # TODO(JohanMuedsam) - How to generalize handling of environment variables
        SPARK_SUBMIT_OPTIONS+=" --conf spark.yarn.appMasterEnv.http_proxy=${HTTP_PROXY} --conf spark.yarn.appMasterEnv.https_proxy=${HTTPS_PROXY} --conf spark.yarn.appMasterEnv.HTTPS_PROXY=${HTTPS_PROXY} --conf spark.yarn.appMasterEnv.HTTPS_PROXY=${HTTPS_PROXY} --conf spark.yarn.executorEnv.PYTHONPATH=${PYTHONPATH} --conf spark.yarn.appMasterEnv.PYTHONPATH=${PYTHONPATH} --conf spark.yarn.executorEnv.PYSPARK_PYTHON=${PYSPARK_PYTHON} --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=${PYSPARK_PYTHON} --conf spark.executorEnv.SPARK_HOME=${SPARK_HOME} --conf spark.yarn.appMasterEnv.SPARK_HOME=${SPARK_HOME} --conf spark.executorEnv.ZEPPELIN_HOME=${ZEPPELIN_HOME} --conf spark.yarn.appMasterEnv.ZEPPELIN_HOME=${ZEPPELIN_HOME} --files ${SPARK_HOME}/conf/hive-site.xml"
        ${SPARK_SUBMIT} --master yarn-cluster --class ${ZEPPELIN_SERVER} --conf spark.driver.extraClassPath="${ZEPPELIN_INTP_CLASSPATH_OVERRIDES}:${CLASSPATH}" --conf spark.driver.extraJavaOptions="${JAVA_INTP_OPTS}" ${SPARK_SUBMIT_OPTIONS} ${SPARK_APP_JAR} ${HOST} ${PORT} &
    else
     
        ${SPARK_SUBMIT} --class ${ZEPPELIN_SERVER} --driver-class-path "${ZEPPELIN_INTP_CLASSPATH_OVERRIDES}:${CLASSPATH}" --driver-java-options "${JAVA_INTP_OPTS}" ${SPARK_SUBMIT_OPTIONS} ${SPARK_APP_JAR} ${HOST} ${PORT} &
    fi
else
    ${ZEPPELIN_RUNNER} ${JAVA_INTP_OPTS} ${ZEPPELIN_INTP_MEM} -cp ${ZEPPELIN_INTP_CLASSPATH_OVERRIDES}:${CLASSPATH} ${ZEPPELIN_SERVER} ${HOST} ${PORT} &
fi

pid=$!
if [[ -z "${pid}" ]]; then
  return 1;
else
  echo ${pid} > ${ZEPPELIN_PID}
fi


trap 'shutdown_hook;' SIGTERM SIGINT SIGQUIT
function shutdown_hook() {
  local count
  count=0
  while [[ "${count}" -lt 10 ]]; do
    $(kill ${pid} > /dev/null 2> /dev/null)
    if kill -0 ${pid} > /dev/null 2>&1; then
      sleep 3
      let "count+=1"
    else
      rm -f "${ZEPPELIN_PID}"
      break
    fi
  if [[ "${count}" == "5" ]]; then
    $(kill -9 ${pid} > /dev/null 2> /dev/null)
    rm -f "${ZEPPELIN_PID}"
  fi
  done
}

wait
