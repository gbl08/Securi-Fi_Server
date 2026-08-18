import firebase_admin
from firebase_admin import credentials, firestore

from datetime import datetime
import uuid


# setup:
cred = credentials.Certificate('firebase.json')
firebase_admin.initialize_app(cred)
db = firestore.client()

MAX_CACHE = 50 # cat de multe package-uri vrem sa salvam in cache TODO 
IDLE_CLOSE_COUNT = 30 # cate pachete idle vrem sa inchida o sesiune TODO

print("[DB] Firebase connected :D")


# users:
def create_user_profile(uid: str, email: str, phone: str):
    # firebase tine parola and shit, in firestore tinem minte profilul
    #in firestore se tin chiestiile neimportante like notif pref and shit
    db.collection("users").document(uid).set({
        "email": email,
        "phone": phone,
        "home_id": None,
        "fcm_tocken": None,
        "created_at": datetime.now().isoformat()
    }, merge=True) 

    print(f"[DB]: User profile created: {uid}")

def get_user():
    pass

def set_fcm_token():
    pass