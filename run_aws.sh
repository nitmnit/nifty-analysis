#!/bin/bash
echo "Cron start"
cd ~/nse-reports-trade
source ~/.bashrc
source nse/bin/activate
#git pull
python live_zerodha.py live $(date -d 'this thu' +%Y-%m-%d) $(date +"%Y-%m-%d")
echo "Cron end"
