#encoding=utf-8
#########################################################################
# File Name: sync.sh
# Author: GuoTianyou
# mail: fortianyou@gmail.com
# Created Time: Thu Feb 23 11:37:05 2017
#! /bin/bash

#########################################################################
zip -d out/artifacts/lda_jar/lda.jar 'META-INF/*.SF' 'META-INF/*.RSA' 'META-INF/*SF'
scp -r ./resources ./src ./out root@bda07:/home/gty/LDA

