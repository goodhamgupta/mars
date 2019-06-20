cd /home/ubuntu/mars
echo "Resetting mars"
git fetch
git checkout -f master
git reset --hard origin/master
git clean -df
git submodule update --recursive --force
echo "Setting environment variables"
bash scripts/variables.sh
echo "Restarting services"
sudo systemctl daemon-reload
sudo systemctl restart airflow-webserver
sudo systemctl restart airflow-scheduler
sudo systemctl restart airflow-worker
echo "Setup done"
