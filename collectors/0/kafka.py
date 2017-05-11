#!/usr/bin/python

import os
import re
import signal
import subprocess
import sys
import time

from collectors.lib import utils

# How oftent to poll
INTERVAL = "60"

# If this user doesn't exist, we'll exit immediately.
# If we're running as root, we'll drop privileges using this user.
USER = "root"
JAVA_HOME = os.getenv("JAVA_HOME")
# We add those files to the classpath if they exist.
CLASSPATH = [
    JAVA_HOME + "/lib/tools.jar",
]

# Map certain JVM stats so they are unique and shorter
# JMX_SERVICE_RENAMING = {
#   "GarbageCollector": "gc",
#   "OperatingSystem": "os",
#   "Threading": "threads",
# }

METRICS_RENAMING = {
    "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec": "BytesIn",
    "kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec": "BytesOut",
    "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec": "MessagesIn",
    "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic=": "BytesInPerTopic",
    "kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec,topic=": "BytesOutPerTopic",
    "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=": "MessagesInPerTopic",
    "kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions": "UnderReplicatedPartitions",
    "kafka.controller:type=KafkaController,name=ActiveControllerCount": "ActiveController",
    "kafka.controller:type=ControllerStats,name=LeaderElectionRateAndTimeMs": "LeaderElection",
    "kafka.controller:type=ControllerStats,name=UncleanLeaderElectionsPerSec": "UncleanLeaderElection",
    "kafka.log:type=LogFlushStats,name=LogFlushRateAndTimeMs": "LogFlush",
    "kafka.log:type=Log,name=LogEndOffset,topic=": "LogEndOffset",
    "kafka.log:type=Log,name=LogStartOffset,topic=": "LogStartOffset",
    "kafka.log:type=Log,name=Size,topic=": "LogSize",
    "kafka.network:type=RequestMetrics,name=TotalTimeMs,request=Produce": "Produce",
    "GarbageCollector": "GC",
    "OperatingSystem": "OS",
    "Threading": "Threads"
}


def splitTopicName(mbean):
    index = mbean.find("topic=")
    if index == -1:
        return ()
    else:
            p_index = mbean.find("partition=")
            if p_index == -1:	
                topicName = mbean[index + 6:]
                mbeanName = mbean[0: index + 6]
                return (topicName, mbeanName)
            else:
                mbeanName = mbean[0: index + 6]
                topicName = mbean[index + 6: p_index - 1]
                partitionId = mbean[p_index + 10:]
                return (topicName, partitionId, mbeanName) 


IGNORED_METRICS = frozenset(["Loggers", "MBeanName"])

# How many times, maximum, will we attempt to restart the JMX collector.
# If we reach this limit, we'll exit with an error.
MAX_RESTARTS = 10

TOP = False  # Set to True when we want to terminate.
RETVAL = 0    # Return value set by signal handler.


def kill(proc):
    """Kills the subprocess given in argument."""
    # Clean up after ourselves.
    proc.stdout.close()
    rv = proc.poll()
    if rv is None:
      os.kill(proc.pid, 15)
      rv = proc.poll()
      if rv is None:
          os.kill(proc.pid, 9)  # Bang bang!
          rv = proc.wait()  # This shouldn't block too long.
    print >>sys.stderr, "warning: proc exited %d" % rv
    return rv


def do_on_signal(signum, func, *args, **kwargs):
  """Calls func(*args, **kwargs) before exiting when receiving signum."""
  def signal_shutdown(signum, frame):
    print >>sys.stderr, "got signal %d, exiting" % signum
    func(*args, **kwargs)
    sys.exit(128 + signum)
  signal.signal(signum, signal_shutdown)


def main(argv):
    utils.drop_privileges(user=USER)
    # Build the classpath.
    dir = os.path.dirname(sys.argv[0])
    jar = os.path.normpath(dir + "/../lib/jmx.jar")
    if not os.path.exists(jar):
        print >>sys.stderr, "WTF?!  Can't run, %s doesn't exist" % jar
        return 13 # ask tcollector to not respawn us
    classpath = [jar]
    for jar in CLASSPATH:
        if os.path.exists(jar):
            classpath.append(jar)
    classpath = ":".join(classpath)

    jpid = "Kafka"
    jps = subprocess.check_output("jps").split("\n")
    for item in jps:
      vals = item.split(" ")
      if len(vals) == 2:
        if vals[1] == "Kafka":
          jpid = vals[0]
          break
    jmx = subprocess.Popen(
        ["java", "-enableassertions", "-enablesystemassertions",  # safe++
         "-Xmx64m",  # Low RAM limit, to avoid stealing too much from prod.
         "-cp", classpath, "com.stumbleupon.monitoring.Jmx",
         "--watch", INTERVAL, "--long", "--timestamp",
         jpid,  # Name of the process.
         # The remaining arguments are pairs (mbean_regexp, attr_regexp).
         # The first regexp is used to match one or more MBeans, the 2nd
         # to match one or more attributes of the MBeans matched.
         "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec",
         "OneMinuteRate|Count",
         "kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec",
         "OneMinuteRate|Count",
         "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec",
         "OneMinuteRate|Count",
         "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic=*",
         "OneMinuteRate|Count",
         "kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec,topic=*",
         "OneMinuteRate|Count",
         "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=*",
         "OneMinuteRate|Count",
         "kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions",
         "Value",
         "kafka.controller:type=KafkaController,name=ActiveControllerCount",
         "Value",
         "kafka.controller:type=KafkaController,name=ActiveControllerCount",
         "Value",
         "kafka.controller:type=ControllerStats,name=LeaderElectionRateAndTimeMs",
         "Count|Max",
         "kafka.controller:type=ControllerStats,name=UncleanLeaderElectionsPerSec",
         "Count",
         "kafka.log:type=LogFlushStats,name=LogFlushRateAndTimeMs",
         "OneMinuteRate|75thPercentile|99thPercentile|999thPercentile",
         "kafka.log:type=Log,name=Size,topic=[^,]*,partition=[^,]*",
         "Value",
         "kafka.log:type=Log,name=LogEndOffset,topic=[^,]+,partition=[^,]+",
         "Value",
         "kafka.log:type=Log,name=LogStartOffset,topic=[^,]+,partition=[^,]+",
         "Value",
         "kafka.network:type=RequestMetrics,name=TotalTimeMs,request=Produce",
         "Mean|75thPercentile|99thPercentile|999thPercentile",
         "Threading", "ThreadCount",       # Number of threads and CPU time.
         "OperatingSystem", "OpenFile",    # Number of open files.
         "GarbageCollector", "Collection", # GC runs and time spent GCing.
         ], stdout=subprocess.PIPE, bufsize=1)
    do_on_signal(signal.SIGINT, kill, jmx)
    do_on_signal(signal.SIGPIPE, kill, jmx)
    do_on_signal(signal.SIGTERM, kill, jmx)
    try:
        prev_timestamp = 0
        while True:
            line = jmx.stdout.readline()
            if not line and jmx.poll() is not None:
                print "not line and jmx.poll() is not None"
                break  # Nothing more to read and process exited.
            elif len(line) < 4:
                print >>sys.stderr, "invalid line (too short): %r" % line
                continue
            timestamp, metric, value, mbean = line.rstrip().split("\t", 3)
         #   print >>sys.stderr, timestamp, "|", metric, "|", value, "|", mbean
            # # Sanitize the timestamp.
            try:
                timestamp = int(timestamp)
                if timestamp < time.time() - 600:
                    raise ValueError("timestamp too old: %d" % timestamp)
                if timestamp < prev_timestamp:
                    raise ValueError("timestamp out of order: prev=%d, new=%d"
                                     % (prev_timestamp, timestamp))
            except ValueError, e:
                print >>sys.stderr, ("Invalid timestamp on line: %r -- %s"
                                     % (line, e))
                continue
            prev_timestamp = timestamp
            # if metric in IGNORED_METRICS:
            #   continue
            #
            # The JMX metrics have per-request-type metrics like so:
            #   metricNameNumOps
            #   metricNameMinTime
            #   metricNameMaxTime
            #   metricNameAvgTime
            # Group related metrics together in the same metric name, use tags
            # to separate the different request types, so we end up with:
            #   numOps op=metricName
            #   avgTime op=metricName
            # etc, which makes it easier to graph things with the TSD.
            # if metric.endswith("MinTime"):  # We don't care about the minimum
            #     continue                    # time taken by operations.
            # elif metric.endswith("NumOps"):
            #     tags = " op=" + metric[:-6]
            #     metric = "numOps"
            # elif metric.endswith("AvgTime"):
            #     tags = " op=" + metric[:-7]
            #     metric = "avgTime"
            # elif metric.endswith("MaxTime"):
            #     tags = " op=" + metric[:-7]
            #     metric = "maxTime"

            # # mbean is of the form "domain:key=value,...,foo=bar"
            # # some tags can have spaces, so we need to fix that.
            # mbean_domain, mbean_properties = mbean.rstrip().replace(" ", "_").split(":", 1)
            # mbean_domain = mbean_domain.rstrip().replace("\"", "");
            # mbean_properties = mbean_properties.rstrip().replace("\"", "");
            #
            # # if mbean_domain not in ("kafka.server",  "kafka.controller", "kafka.network", "kafka.log", "java.lang"):
            # #     print >>sys.stderr, ("Unexpected mbean domain = %r on line %r"
            # #                          % (mbean_domain, line))
            # #     continue
            #
            # mbean_properties = dict(prop.split("=", 1)
            #                         for prop in mbean_properties.split(","))
            #
            # if mbean_domain == "kafka.cluster":
            #     jmx_service = mbean_properties.get("type", "cluster");
            #     if mbean_properties:
            #         tags += " " + " ".join(k + "=" + v for k, v in
            #                                mbean_properties.iteritems())
            # elif mbean_domain == "kafka.controller":
            #     jmx_service = mbean_properties.get("type", "controller");
            #     if mbean_properties:
            #         tags += " " + " ".join(k + "=" + v for k, v in
            #                                mbean_properties.iteritems())
            # elif mbean_domain == "kafka.network":
            #     jmx_service = mbean_properties.get("type", "network");
            #     if mbean_properties:
            #         tags += " " + " ".join(k + "=" + v for k, v in
            #                                mbean_properties.iteritems())
            # elif mbean_domain == "kafka.server":
            #     jmx_service = mbean_properties.get("type", "server");
            #     if mbean_properties:
            #         tags += " " + " ".join(k + "=" + v for k, v in
            #                                mbean_properties.iteritems())
            # elif mbean_domain == "kafka.log":
            #     jmx_service = mbean_properties.get("type", "log");
            #     if mbean_properties:
            #         tags += " " + " ".join(k + "=" + v for k, v in
            #                                mbean_properties.iteritems())
            # elif mbean_domain == "kafka.consumer":
            #     jmx_service = mbean_properties.pop("type", "consumer")
            #     if mbean_properties:
            #         tags += " " + " ".join(k + "=" + v for k, v in
            #                                mbean_properties.iteritems())
            # elif mbean_domain == "java.lang":
            #     jmx_service = mbean_properties.pop("type", "jvm")
            #     if mbean_properties:
            #         tags += " " + " ".join(k + "=" + v for k, v in
            #                                mbean_properties.iteritems())
            # else:
            #     assert 0, "Should never be here"
            try:
                tags = ""
                mbean = mbean.replace(" ", "_")
                topic_mbean = splitTopicName(mbean)
	  #      print >>sys.stderr, mbean
	  #      print >>sys.stderr, topic_mbean
                if(topic_mbean != ()):
                    if(len(topic_mbean) == 2):
                        topic, mbean = topic_mbean
                        tags += " " + "topic=" + topic
                    else:
                        topic, partition, mbean = topic_mbean 
                        tags += " " + "topic=" + topic + " " + "partition=" + partition
                #print >>sys.stderr, "mbean: " + mbean
                short_name = METRICS_RENAMING.get(mbean, mbean)
                metric_name = "kafka." + short_name + "." + metric
                metric_name = metric_name.replace("java.lang:type=", "")
                #metric_name = metric_name.replace(":", ".")
                metric_name = metric_name.replace(",name=", ".")
                sys.stdout.write("%s %d %s%s\n" % (metric_name, timestamp, value, tags))
                sys.stderr.write("%s %d %s%s\n" % (metric_name, timestamp, value, tags))
                sys.stdout.flush()
            except Exception as e:
                print >>sys.stderr, e
    finally:
        kill(jmx)
        time.sleep(60)
        return 0  # Ask the tcollector to re-spawn us.

if __name__ == "__main__":
    sys.exit(main(sys.argv))

