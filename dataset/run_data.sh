#! /usr/bin/env bash

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Sandman
#     Start Bash DataSet
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
rm -r json_files
mkdir json_files
cd json_files
mkdir book
cd ..

cd script
python main.py
cd ..
rm -r json_files


