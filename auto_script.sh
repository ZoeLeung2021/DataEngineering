bash install_spark.sh
pip install vaderSentiment
pip install dvc


dvc exp run -f game_results_checks
dvc exp run -f Athlete_personal_information_checks
dvc exp run -f STK_tweets_checks
dvc exp run -f data_transformation_checks
dvc exp run -f write_into_database_checks

dvc exp show