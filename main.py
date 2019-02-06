import base64
import io
import json
import uuid

from flask import Flask,redirect,send_file
from google.cloud import datastore,exceptions,storage
import sendgrid
from sendgrid.helpers import mail


app = Flask(__name__)

DS_CLIENT = datastore.Client()
GCS_CLIENT = storage.Client()

key = DS_CLIENT.key('gift', 'config')
config = DS_CLIENT.get(key)
PROJECT_NAME = config['project']
BUCKET_NAME = config['bucket']
APP_URL = config['url']

SG_CLIENT = sendgrid.SendGridAPIClient(apikey=config['sendgrid_api'])
SENDER = mail.Email(config['sender'])
RECIPIENT = mail.Email(config['recipient'])


@app.route('/')
def start():
  return str(uuid.uuid4())

@app.route('/scantag/<auuid>')
def scanTag(auuid):
  return redirect('zxing://scan/?ret=https%3A%2F%2F{}%2Fnewasset%2F{}%2F%7BCODE%7D&SCAN_FORMATS=CODE_39'.format(APP_URL, auuid))

@app.route('/newasset/<auuid>/<tag>')
def newAsset(auuid, tag):
  bucket = None
  desired_bucket_name = auuid
  while not bucket:
    try:
      bucket = GCS_CLIENT.create_bucket(desired_bucket_name)
      policy = bucket.get_iam_policy()
      policy['roles/storage.objectCreator'] = {'allUsers'}
      bucket.set_iam_policy(policy)
    except exceptions.Conflict:
      for b in GCS_CLIENT.list_buckets(project=PROJECT_NAME):
        if b.name == desired_bucket_name:
          bucket = b
      desired_bucket_name = str(uuid.uuid4())

  key = DS_CLIENT.key('sessions', auuid)
  entity = datastore.Entity(key=key)
  entity['path'] = 'gs://{}/{}'.format(bucket.name, tag)
  DS_CLIENT.put(entity)
  return 'ok'

@app.route('/path/<auuid>')
def getPath(auuid):
  key = DS_CLIENT.key('sessions', auuid)
  entity = DS_CLIENT.get(key)
  return entity['path']

@app.route('/finish/<auuid>')
def finish(auuid):
  key = DS_CLIENT.key('sessions', auuid)
  entity = DS_CLIENT.get(key)
  src_bucket_name = entity['path'].split("/")[2]

  src_bucket = GCS_CLIENT.get_bucket(src_bucket_name)
  dst_bucket = GCS_CLIENT.get_bucket(BUCKET_NAME)
  blobs = src_bucket.list_blobs()

  for blob in blobs:
    src_bucket.copy_blob(blob,dst_bucket)

  src_bucket.delete(force=True)
  DS_CLIENT.delete(key)
  
  subject = 'Acquisition Complete ({})'.format(entity['path'].split("/")[3])
  content = mail.Content("text/plain", entity['path'])
  msg = mail.Mail(SENDER, subject, RECIPIENT, content)
  response = SG_CLIENT.client.mail.send.post(request_body=msg.get())

  return 'ok'


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)
