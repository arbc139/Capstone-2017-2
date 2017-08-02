while true; do
	sleep $sleep_time;
	curr_date=$(date +"%Y-%m-%d %H:%M:%S")
	sleep_time="1.0"
	(echo "")&
	(echo "[$curr_date] New Job Check";python new_job.py)&
	(echo "[$curr_date] Insert Check"; python insertor.py)&
	(echo "[$curr_date] Weka Check"; python weka_start.py)&
	(echo "[$curr_date] Net Check"; python net_start.py)&

done
