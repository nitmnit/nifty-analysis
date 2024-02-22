#!/bin/bash
cd ~/nse-reports-trade
source ~/.bashrc
source nse/bin/activate
python live_zerodha.py
