#!/usr/bin/env python3
# coding: utf-8

import os

datalake_bucket = os.environ["$DATALAKE_BUCKET"]

datalake_path = "s3a://" + datalake_bucket
datalake_raw_path = datalake_path + "/raw/jhu_csse_covid_19"
datalake_staged_path = datalake_path + "/staged/jhu_csse_covid_19"
datalake_features_path = datalake_path + "/features/jhu_csse_covid_19"

staged_countries = ["Tunisia", "France", "China", "Italy"]
daily_feature_countries = ["Tunisia", "France"]
