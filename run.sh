#!/bin/zsh
cd ~/nse-reports-trade
source ~/.zshrc
conda activate nse
python live_zerodha.py
