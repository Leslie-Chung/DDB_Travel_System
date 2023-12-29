PIDS=$(ps aux | grep "DrmiPort=8080" | grep -v grep | awk '{print $2}')
# 遍历并杀死每个进程
for PID in $PIDS; do
    kill -9 $PID
    echo "Killed process with PID $PID"
done

PIDS=$(lsof -t -i :8080)
# 遍历并杀死每个进程
for PID in $PIDS; do
    kill -9 $PID
    echo "Killed process with PID $PID"
done