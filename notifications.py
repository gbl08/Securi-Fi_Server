from firebase_admin import messaging
from database import get_user_profile, db

def get_notification_content(alarm_type: str, probability: float) -> tuple[str, str]:
    match alarm_type:
        case "intruder":
            return (
                "Securi-Fi: Intruder detected",
                f"Probability: {probability:.0%}. Open the app for more details"
            )
        case "gas_leak":
            return (
                "Securi-Fi: Gas detected",
                "The gas alarm was set off in your house"
            )
        case "fire":
            return (
                "Securi-Fi: Fire detected",
                "Flames were detected in your house"
            )
        case _:
            return (
                "Securi-Fi: Alert",
                "Something is happening in your house, open the app for more details"
            )

async def send_to_token(token: str, title: str, body: str) -> bool:
    if not token:
        return False

    message = messaging.Message(
        notification=messaging.Notification(title=title, body=body),
        token=token,
        android=messaging.AndroidConfig(priority="high"),
        apns=messaging.APNSConfig(headers={"apns-priority": "10"})
    )

    try:
        messaging.send(message)
        return True
    except messaging.UnregisteredError:
        print(f"[FCM] Token expired / unregistered: {token[:20]}...")
        return False
    except messaging.SenderIdMismatchError:
        print(f"[FCM] Sender ID mismatch. Check serviceAccountKey.json")
        return False
    except Exception as e:
        print(f"[FCM] Failed: {e}")
        return False

async def notify_home(hid: str, alert_type: str, probability: float):

    title, body = get_notification_content(alert_type, probability)

    links = db.collection("userHomeLinks").where("hid", "==", hid).stream()
    uids = [link.to_dict().get("uid") for link in links]

    if not uids:
        print(f"[FCM] No users linked to home {hid}")
        return

    sent = 0
    for uid in uids:
        profile = get_user_profile(uid)
        if not profile:
            continue

        tokens = profile.get("fcmTokens", [])
        if not tokens:
            print(f"[FCM] No tokens for user {uid}")
            continue

        for token in tokens:
            success = await send_to_token(token, title, body)
            if success:
                sent += 1

    print(f"[FCM] Notifications sent to {sent} device(s) for home {hid} | {alert_type}")