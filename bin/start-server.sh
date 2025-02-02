!/bin/bash
# start SNDML as an HTTP server background process
# file names are relative to the current working directory

# verify that these 3 variables are correct
export SNDML_PROFILE=sndml.profile
export SNDML_JAR=sndml.jar
export SNDML_LOG_DIR=log

# extract these 3 variables from the connection profile
export SNDML_AGENT=`awk -F= '/\.agent=/{print $2}' <$SNDML_PROFILE`
export SNDML_PORT=`awk -F= '/\.port=/{print $2}' <$SNDML_PROFILE`
export SNDML_PIDFILE=`awk -F= '/\.pidfile=/{print $2}' <$SNDML_PROFILE`

OPT_LOG="-Dlog4j2.configurationFile=log4j2-daemon.xml -Dsndml.logFolder=$SNDML_LOG_DIR -Dsndml.logPrefix=$SNDML_AGENT"
CMD="java -ea $OPT_LOG -jar $SNDML_JAR --profile=$SNDML_PROFILE --server"

# print exported variables
env | grep SNDML | sort

# delete the pidfile
if [ -f $SNDML_PIDFILE ]; then
  rm $SNDML_PIDFILE
fi
if [ -f nohup.out ]; then
  rm nohup.out
fi

echo starting $CMD
nohup $CMD &

# wait for pidfile
sleep 3
if [ -f $SNDML_PIDFILE ]; then
  PID=`cat $SNDML_PIDFILE`
  echo Server is running on PID $PID
  exit 0
else 
  cat nohup.out
  exit 1
fi
