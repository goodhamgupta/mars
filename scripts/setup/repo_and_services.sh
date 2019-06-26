cd /home/ubuntu/mars
echo "Resetting mars"
git fetch
git checkout -f master
git pull origin master
echo "Restarting services"
sudo systemctl daemon-reload
sudo systemctl restart airflow-webserver
sudo systemctl restart airflow-scheduler
echo "Setup done"
