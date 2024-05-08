# Merlin

A toy database implementation


```
create table music (Title varchar(100), Artist varchar(100), Rank number, Date varchar(10), Region varchar(15))
import csv from "/home/martinc/spotify.csv" into music with (Title=title, Artist=artist, Rank=rank, Date=date, Region=region)

```