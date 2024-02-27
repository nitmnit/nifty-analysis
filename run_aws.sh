#!/bin/bash
cd ~/nse-reports-trade
source ~/.bashrc
source nse/bin/activate
python live_zerodha.py no_live $(date -d "next Thursday" +%Y-%m-%d) $(date +"%Y-%m-%d")
