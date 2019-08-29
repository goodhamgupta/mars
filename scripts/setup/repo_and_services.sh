cd /home/ubuntu/mars
echo "Resetting mars"
git fetch
git checkout -f master
git pull origin master
echo "Installing custom packages"
cd /home/ubuntu/mars/hiveql
sudo pip3 install .
cd /home/ubuntu/mars/dags/common
sudo pip3 install .
echo "Restarting services"
sudo systemctl daemon-reload
sudo systemctl restart airflow-webserver
sudo systemctl restart airflow-scheduler
echo "Setup done"
