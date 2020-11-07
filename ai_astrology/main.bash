#!/bin/bash
conda init bash
conda activate brain
python3 ai_astrology/astrology.py && python3 ai_astrology/astropost.py 
