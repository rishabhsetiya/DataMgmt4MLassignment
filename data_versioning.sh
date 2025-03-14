mkdir versioned_data
cp ./data/raw/api/api_data.csv ./versioned_data/api_data.csv
cp ./data/raw/static/static_data.csv ./versioned_data/static_data.csv
dvc add versioned_data/api_data.csv
dvc add versioned_data/static_data.csv

git add versioned_data/api_data.csv.dvc versioned_data/.gitignore
git add versioned_data/static_data.csv.dvc versioned_data/.gitignore

mkdir ./tmp/dvcstore
dvc remote add -d myremote ./tmp/dvcstore

dvc push
git commit versioned_data -m "raw data"

cp ./data/processed/api/api_data.csv ./versioned_data/api_data.csv
cp ./data/processed/static/static_data.csv ./versioned_data/static_data.csv

dvc add versioned_data/api_data.csv
dvc add versioned_data/static_data.csv
dvc push
git commit versioned_data -m "processed data"

git checkout HEAD~1
dvc checkout



