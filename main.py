import base64
import io
import json
import uuid

from flask import Flask,redirect
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

SG_CLIENT = sendgrid.SendGridAPIClient(config['sendgrid_api'])
SENDER = config['sender']
RECIPIENT = config['recipient']


@app.route('/')
def start():
  return str(uuid.uuid4())


@app.route('/scantag/<auuid>')
def scanTag(auuid):
  return redirect('zxing://scan/?ret=https%3A%2F%2F{}%2Fnewasset%2F{}%2F%7BCODE%7D&SCAN_FORMATS=CODE_39'.format(APP_URL, auuid))


@app.route('/newasset/<auuid>/<tag>')
def newAsset(auuid, tag):
  skey = DS_CLIENT.key('sessions', auuid)
  entity = datastore.Entity(key=skey)
  entity['path'] = 'gs://{}/{}'.format(BUCKET_NAME, tag)
  DS_CLIENT.put(entity)

  subject = 'Acquisition Underway ({})'.format(tag)
  content = 'Image acquisition for {} has begun.'.format(tag)
  msg = mail.Mail(SENDER, RECIPIENT, subject, content)
  SG_CLIENT.send(msg)
  return 'ok'


@app.route('/path/<auuid>')
def getPath(auuid):
  skey = DS_CLIENT.key('sessions', auuid)
  entity = DS_CLIENT.get(skey)
  return entity['path']


@app.route('/finish/<auuid>')
def finish(auuid):
  skey = DS_CLIENT.key('sessions', auuid)
  entity = DS_CLIENT.get(skey)
  asset = entity['path'].split('/')[3]
  DS_CLIENT.delete(skey)

  subject = 'Acquisition Complete ({})'.format(asset)
  content = 'Image acquisition complete: gs://{}/{}'.format(
          BUCKET_NAME, asset)
  msg = mail.Mail(SENDER, RECIPIENT, subject, content)
  SG_CLIENT.send(msg)
  return 'ok'


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)

