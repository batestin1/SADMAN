#! /usr/bin/env bash

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Sandman
#     Start Bash DataSet
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
cd dataset
sh run_data.sh
cd ..

echo "STARTING PROCESSING DATE"

cd scripts
python querys_bronze.py

echo "NORMALIZING THE DATA"
python querys_silver.py

echo "UNIFYING THE DATA"

python querys_gold.py

echo "ENDING THE PROCESS AND SAVING TO THE DATABASE"
python querys_final.py

