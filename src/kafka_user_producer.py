#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug  8 13:39:48 2017

@author: alvaro
"""

from kafka import KafkaProducer
import random
from time import sleep
import sys, os
import unicodedata
from nltk.corpus import stopwords 
import re
import string

def filter_words(tweet):
    tweet_filtered = ''
    for word in tweet.split():
        try:
            if word[0] != '@' and word[0] != '#' and word.lower() not in spanish_stopwords:
                   # Remove repeat character, there are some words like 'creemos' that must be get in count
                   rep_let = [i+i for i in re.findall(r'([a-z])\1', word)]
                   if rep_let.count('ee') > 1:
                       word = re.sub(r'([e])\1+', r'\1', word)
                   if rep_let.count('oo') > 1:
                       word = re.sub(r'([o])\1+', r'\1', word)
    
                   word = re.sub(r'([a+i+u+s+j])\1+', r'\1', word)
    
    
                   # Remove puntuation characters
                   aux2 = ''
                   for letter in word:
                       if letter not in string.punctuation and letter != '”' and letter != '“' and letter != '¡' and letter!= '¨' and letter != '«' and letter != '»' and letter != '¿':
                           aux2 = aux2 + letter
                       else:
                           aux2 = aux2 + ' '
    #               aux = ''.join([letter for letter in aux if letter not in string.punctuation and letter != '”' and letter != '“' and letter != '¡' and letter!= '¨' and letter != '«' and letter != '»' and letter != '¿'])
                   if 'ñ' not in aux2.lower():
                       words_no_accent = ''.join((c for c in unicodedata.normalize('NFD', aux2) if unicodedata.category(c) != 'Mn'))
                   else:
                       # Letter Ñ can't be deleted
                       words_no_accent = aux2               
                   tweet_filtered = tweet_filtered + ' ' + words_no_accent
                   
        except ValueError:
            return None
    return tweet_filtered.strip()

def word_embedding(word):

    first_line = True
    with open("SBW-vectors-300-min5.txt", 'r') as infile:
        for line in infile:
            if first_line:
                first_line = False
            else:        
                words = line.split()
                words[0] = ''.join((c for c in unicodedata.normalize('NFD', words[0]) if unicodedata.category(c) != 'Mn'))
                if words[0] == word:
                    return words[1:300]
        return None

if __name__=="__main__":
    try:
        spanish_stopwords = stopwords.words('spanish')
        print("Initialization...")
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
    
        print("Sending messages to kafka 'test' topic...")
        topic = sys.argv[1]

        while True:
            print("Introduce your tweet: ")
            tweet = input()
            w2vect = []
            tweet = filter_words(tweet)
        
            for word in tweet.split():
                values = word_embedding(word)
                if values != None:
                    w2vect = w2vect + values

            print("Tweet transformed to word embedding")
            tweet_transformed = ' '.join(w2vect)

    
            print(tweet_transformed)
            producer.send(topic, bytes(tweet_transformed, 'utf8'))
            sleep(1)
            print("-------------------------------------------------------")

    
        print("Waiting to complete delivery...")
        producer.flush()
        print("End")


    except KeyboardInterrupt:
        print('Interrupted from keyboard, shutdown')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
