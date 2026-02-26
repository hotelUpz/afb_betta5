import requests
import time

BOT_TOKEN = "8222176205:AAFnd01zBleDoUO0tyc071ulyw021l7E2-c"

def main():
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"

    print("Жду сообщения в группе...")
    time.sleep(3)

    response = requests.get(url)
    data = response.json()

    if not data.get("ok"):
        print("Ошибка запроса:", data)
        return

    results = data.get("result", [])
    if not results:
        print("Нет обновлений. Напиши что-нибудь в группе и запусти снова.")
        return

    for update in results:
        message = update.get("message") or update.get("channel_post")
        if not message:
            continue

        chat = message.get("chat", {})
        print("\n--- НАЙДЕНО ---")
        print("Title:", chat.get("title"))
        print("Type:", chat.get("type"))
        print("Chat ID:", chat.get("id"))
        print("----------------")

if __name__ == "__main__":
    main()