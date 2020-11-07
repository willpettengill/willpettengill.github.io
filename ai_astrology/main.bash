#!/bin/bash
conda init bash
conda activate brain
python3 astrology.py && python3 astropost.py 
