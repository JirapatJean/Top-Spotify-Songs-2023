#!/bin/bash

kaggle datasets download -d nelgiriyewithana/top-spotify-songs-2023
unzip top-spotify-songs-2023 -d ./data
rm top-spotify-songs-2023.zip