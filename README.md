# Merlin

A toy database implementation


```
create table music (Title varchar(255), Artist varchar(255), Rank number, Date varchar(10))
import csv from "/home/martinc/spotify.csv" into music with (Title=title, Artist=artist, Rank=rank, Date=date) truncate

```