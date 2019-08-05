run barclreact

Windows:
set JAVA_OPTS=-Xms1G -Xmx1G -Xss1M -XX:+UseConcMarkSweepGC -XX:+UseThreadPriorities -XX:ThreadPriorityPolicy=42 -XX:+AggressiveOpts
scala barclreact.jar >> out.log

Linux:
export JAVA_OPTS="-Xms1G -Xmx1G -Xss1M -XX:+UseConcMarkSweepGC -XX:+UseThreadPriorities -XX:ThreadPriorityPolicy=42 -XX:+AggressiveOpts"
nohup scala barclreact.jar &

==========

-- test 1
set JAVA_OPTS=-Xms1G -Xmx1G -Xss1M -XX:+UseConcMarkSweepGC -XX:+UseThreadPriorities -XX:ThreadPriorityPolicy=42 -XX:+AggressiveOpts

-- test 2
set JAVA_OPTS=-Xmx1G -XX:+UseG1GC -XX:MaxGCPauseMillis=2000 -XX:+UseConcMarkSweepGC


--05.08.2019 v1.
[mts@mts ~]$ export JAVA_OPTS="-Xmx300M -XX:+UseG1GC"
[mts@mts ~]$ echo $JAVA_OPTS
-Xmx300M -XX:+UseG1GC

--05.08.2019 v2.
export JAVA_OPTS="-Xmx300M -XX:+UseG1GC"




