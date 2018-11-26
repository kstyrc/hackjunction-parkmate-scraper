import time
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from google.cloud.firestore_v1beta1._helpers import GeoPoint
import untangle
from random import randint
import traceback


def get_db(path):
    cred = credentials.Certificate(path)
    firebase_admin.initialize_app(cred)
    return firestore.client()


def query_parking_lots(url):
    obj = untangle.parse(url)
    parklot_by_id = {}
    for item in obj.d2LogicalModel.payloadPublication.genericPublicationExtension.parkingFacilityTableStatusPublication.parkingFacilityStatus:
        id = item.parkingFacilityReference['id']
        if id:
            parklot = parklot_by_id.get(id, {})
            if type(item.parkingFacilityStatus) is not list:
                parklot['status'] = item.parkingFacilityStatus.cdata
            else:
                parklot['status'] = item.parkingFacilityStatus[0].cdata
            parklot_by_id[id] = parklot

    for item in obj.d2LogicalModel.payloadPublication.genericPublicationExtension.parkingFacilityTablePublication.parkingFacilityTable.parkingFacility:
        id = item['id']
        parklot = parklot_by_id.get(id)
        parklot['name'] = item.parkingFacilityName.values.value.cdata
        parklot['latitude'] = float(item.entranceLocation.pointByCoordinates.pointCoordinates.latitude.cdata)
        parklot['longitude'] = float(item.entranceLocation.pointByCoordinates.pointCoordinates.longitude.cdata)

    return parklot_by_id


def update_parking_lots(parklot_by_id, collection):
    for id in parklot_by_id:
        parklot = parklot_by_id[id]
        spaces = randint(5, 50)
        free_spaces = int(spaces * 0.5) if parklot_by_id.get('status') != 'full' else 0
        doc_ref = collection.document(id)
        doc_ref.set({
            u'name': parklot['name'],
            u'spaces': spaces,
            u'free_spaces': free_spaces,
            u'location': GeoPoint(latitude=parklot['latitude'], longitude=parklot['longitude'])
        })


def main(url, path, collection_name):
    db = get_db(path)
    while True:
        try:
            print('Querying Tampere Parking Open Data')
            parklot_by_id = query_parking_lots(url)

            print('Got info about {} parking lots'.format(len(parklot_by_id)))
            update_parking_lots(parklot_by_id, db.collection(collection_name))

            print('Updating done, sleeping for 5 mins')
            time.sleep(5*60)
        except Exception:
            traceback.print_exc()


if __name__ == "__main__":
    main('http://parkingdata.finnpark.fi:8080/Datex2/OpenData', './firebase-auth.json', u'parklots')
