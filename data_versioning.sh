# create directory to store data to be version controlled
mkdir versioned_data

# copy raw data to versioned_data
cp ./data/raw/api/api_data.csv ./versioned_data/api_data.csv
cp ./data/raw/static/static_data.csv ./versioned_data/static_data.csv

# add files to dvc
dvc add versioned_data/api_data.csv
dvc add versioned_data/static_data.csv

# add dvc files to git and actual data to .gitignore
git add versioned_data/api_data.csv.dvc versioned_data/.gitignore
git add versioned_data/static_data.csv.dvc versioned_data/.gitignore

# specify remote storage
mkdir ./tmp/dvcstore
dvc remote add -d myremote ./tmp/dvcstore

# push data to remote storage
dvc push
# commit changes to git
git commit versioned_data -m "raw data"

# copy processed data to replace raw data
cp ./data/processed/api/api_data.csv ./versioned_data/api_data.csv
cp ./data/processed/static/static_data.csv ./versioned_data/static_data.csv

# add updated files to dvc
dvc add versioned_data/api_data.csv
dvc add versioned_data/static_data.csv

# push updated data to remote storage and commit changes to git
dvc push
git commit versioned_data -m "processed data"

# checkout previous version of data
git checkout HEAD~1
dvc checkout



