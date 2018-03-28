import os
import sys
import json
import urllib
import logging
import requests
import ratelimiter
from string import Template

import azure
from azure.storage.blob import BlockBlobService

REGION_ID = "westeurope"
STORAGE_CONTAINER_NAME = "faces"
SECRETS = json.loads(open("./secrets.json", "r").read())

class CognitiveServicesClient:
  def __init__(self, api_key):
    self._api_key = None 

  def put(self, url, json):
    print(url)
    headers = { "Ocp-Apim-Subscription-Key" : self._api_key }
    response = requests.put(url, headers = headers, json = json)
    return json.loads(response.text)

  def get(self, url):
    print(url)
    headers = { "Ocp-Apim-Subscription-Key" : self._api_key }
    response = requests.get(url, headers = headers)
    return json.loads(response.text)

  def post(self, url, json = None):
    print(url)
    headers = { "Ocp-Apim-Subscription-Key" : self._api_key }
    response = requests.post(url, headers = headers, json = json)
    return json.loads(response.text)

  def delete(self, url):
    print(url)
    headers = { "Ocp-Apim-Subscription-Key" : self._api_key }
    response = requests.delete(url, headers = headers)
    return json.loads(response.text)


class PersonGroup(CognitiveServicesClient):
  def __init__(self, id = None):
    super().__init__(SECRETS["COGNITIVE_KEY"])
    self._id = None
  
  def createPersonGroup(self, id, userData = ""):
    url = Template(
      "https://${regionId}.api.cognitive.microsoft.com/face/v1.0/persongroups/${personGroupId}"
    )
    url = url.substitute(regionId = REGION_ID, personGroupId = id)
    response = self.put(url, { "name" : id, "userData" : userData })
    self._id = id
    return response

  def list(self):
    url = Template("https://${regionId}.api.cognitive.microsoft.com/face/v1.0/persongroups")
    url = url.substitute(regionId = REGION_ID)
    return self.get(url)

  def getPerson(self):
    url = Template(
      "https://${regionId}.api.cognitive.microsoft.com/face/v1.0/persongroups/${personGroupId}/persons/${personId}"
    ). = url.substitute(regionId = REGION_ID, personGroupId = self, personId = personId)
    return self.get(url)

  def delete(self):
    url = Template(
      "https://${regionId}.api.cognitive.microsoft.com/face/v1.0/persongroups/${personGroupId}"
    ).substitute(regionId = REGION_ID, personGroupId = self._id)
    response = self.delete(url)
    self._id = None
    return response

  def train(self):
    url = Template(
      "https://${regionId}.api.cognitive.microsoft.com/face/v1.0/persongroups/${personGroupId}/train"
    ).substitute(regionId = REGION_ID, personGroupId = self._id)
    return self.post(url, headers = headers)

  def addPerson(self, personName, personData = ""):
    url = Template(
      "https://${regionId}.api.cognitive.microsoft.com/face/v1.0/persongroups/${personGroupId}/persons"
    ).substitute(regionId = REGION_ID, personGroupId = self._id)
    return self.post(url, json = { "name" : personName, userData : personData })

  def getPerson(self, personId):
    url = Template(
      "https://${regionId}.api.cognitive.microsoft.com/face/v1.0/persongroups/${personGroupId}/persons/${personId}"
    ).substitute(regionId = REGION_ID, personGroupId = self._id, personId = personId)
    return self.get(url)

  def addFace(self, personId, faceUrl):
    url = Template(
      "https://${regionId}.api.cognitive.microsoft.com/face/v1.0/persongroups/${personGroupId}/persons/${personId}/persistedFaces"
    ).substitute(regionId = REGION_ID, personGroupId = self._id, personId = personId)
    return requests.post(url, json = { "url" : faceUrl })

  def _detectFace(faceUrl):
    url = Template(
      "https://${regionId}.api.cognitive.microsoft.com/face/v1.0/detect"
    ).substitute(regionId = REGION_ID)
    return self.post(url, json = { "url" : faceUrl })

  def identifyFace(self, faceUrl):
    detectResponse = self._detectFace(faceUrl)
    url = Template(
      "https://${regionId}.api.cognitive.microsoft.com/face/v1.0/identify"
    ).substitute(regionId = REGION_ID)
    return self.post(url, json = { "faceIds" : [ faceId ], "personGroupId" : self._id })


class VideoIndexer(CognitiveServicesClient):
  def __init__(self):
    super().__init__(SECRETS["VIDEO_INDEXER_KEY"])
  
  def labelFace(breakdownId, faceId, toName):
    url = Template(
    "https://videobreakdown.azure-api.net/Breakdowns/Api/Partner/Breakdowns/UpdateFaceName/${id}?faceId=${faceId}&newName=${toName}"
    ).substitute(id = breakdownId, faceId = faceId, toName = toName)
    return self.put(url)


def getHeadshots():
  blob_service = BlockBlobService(
    account_name = SECRETS["STORAGE_ACCOUNT_NAME"], sas_token = SECRETS["STORAGE_SAS_TOKEN"]
  )
  blobs = blob_service.list_blobs(STORAGE_CONTAINER_NAME)
  return map(lambda x: blob_service.make_blob_url(STORAGE_CONTAINER_NAME, x.name), blobs)

_groupId = "oireachtas"

def populatePersonGroup():
  pg = PersonGroup(_groupId)
  pg.delete()
  pg.list()
  headshots = getHeadshots()
  for head in headshots:
    personName = os.path.basename(head).split(".")[0].replace(" [No Logo]", "").split(",")
    personName.reverse()
    personName = " ".join(name).strip()
    personId = pg.addPerson(personName)
    pg.addFace(personId, head)

def labelFaces(jsonInput):
  faces = jsonInput["summarizedInsights"]["faces"]
  breakdownId = jsonInput["breakdowns"][0]["id"]

  vi = VideoIndexer()
  pg = PersonGroup(_groupId)
  rate_limiter = ratelimiter.RateLimiter(max_calls = 10, period = 60)

  for face in faces:
    with rate_limiter:
      results = pg.identifyFace(face["thumbnailFullUrl"])
      if len(results[0]["candidates"]):
        personId = results[0]["candidates"][0]["personId"]
        name = pg.getPerson(personId)["name"]
        vi.labelFace(breakdownId, face["id"], name)

def main():
  jsonInput = json.loads(open("input.json", "r").read())
  labelFaces(jsonInput)

if __name__ == "__main__":
  main()
