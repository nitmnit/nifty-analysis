#!/bin/bash
cd ~/nse-reports-trade
source ~/.bashrc
source nse/bin/activate
git pull
python live_zerodha.py live $(date -d "next Thursday" +%Y-%m-%d) $(date +"%Y-%m-%d")
