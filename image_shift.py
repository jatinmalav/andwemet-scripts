from pymongo import MongoClient
import boto3
import random
import string
import base64


def random_string(length):
    pool = "abcdefghijklmnopqrstuvwxyz1234567890"
    return ''.join(random.choice(pool) for i in range(length))


# Variables
# Fill these variables
MONGO_URI = "mongodb://localhost:27017/andwemet-stage-blog"
DATABASE = "andwemet-stage-blog"
COLLECTION = "posts"
AWS_BUCKET_NAME="andwemet-file"
AWS_END_POINT="https://sgp1.digitaloceanspaces.com"
AWS_PHOTO_DIR="blog"
AWS_ACCESS_KEY_ID="VEPVVX72YKOEQ5TZ7GQW"
AWS_SECRET_ACCESS_KEY="sKYCsiV0bgaKQhGpJ2CGwPq6D6PtWNXhSzxbW1jfLsw"
TEMP_IMAGE_DIR = "tmp/"

mongoCl = MongoClient(MONGO_URI)

db = mongoCl[DATABASE]
collection = db[COLLECTION]

session = boto3.session.Session()

s3_client = session.client(
    service_name='s3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    endpoint_url=AWS_END_POINT,
)

for x in collection.find():
    if x["thumbnail"][0:10] == "data:image":
        extension = ".jpeg"
        if x["thumbnail"][11:14] == "png":
            extension = ".png"
        file_name = "uploads_" + str(random_string(32)) + extension
        with open(TEMP_IMAGE_DIR + file_name, "wb") as new_file:
            new_file.write(base64.decodebytes(
                str(x["thumbnail"][22:]).encode("utf-8")))

        key = AWS_PHOTO_DIR + "/" + file_name
        s3_client.upload_file(TEMP_IMAGE_DIR + file_name, AWS_BUCKET_NAME, key,
        ExtraArgs={'ContentType': "image/"+extension[1:4],'ACL': 'public-read'})
        url = AWS_END_POINT + "/" + AWS_BUCKET_NAME + "/" + key
        post_query = {"_id": x["_id"]}
        update_query = {"$set": { "thumbnail": url }}
        collection.update_one(post_query, update_query)