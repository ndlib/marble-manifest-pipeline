
bucket="marble-manifest-prod-processbucket-kskqchthxshg"

# 1999.024 Momento mori one image
# 1952.019 Diana multiple images from snite
#     - Missing collection record
# 002097132 Aleph Book
# 004862474  Aleph one image
# MSNCOL8501_EAD  AS Letters
ids=("1999.024" "1952.019" "002097132" "004862474" "MSNCOL8501_EAD")

for id in "${ids[@]}"
do
   :
   aws s3 cp  "s3://${bucket}/json/${id}.json" "./${id}/${id}.json"
   aws s3 cp  "s3://${bucket}/csv/${id}.csv" "./${id}/${id}.csv"
   aws s3 cp  "s3://${bucket}/process/${id}/image_data.json" "./${id}/image_data.json"
   aws s3 cp  "s3://${bucket}/process/${id}/metadata/manifest/index.json" "./${id}/manifest.json"
done
