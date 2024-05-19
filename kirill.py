import telebot
from random import randint
import time
import requests
import json


token = "7169631341:AAGrIPNCMDmtsRvAFEgHstPHnoAu-PptePw"
bot = telebot.TeleBot(token)


@bot.message_handler(content_types='text')
def start_message(message):
    print(message.text)
    l=message.text
    l=l.lower()
    g=l.find('чай')
    if g>=0:
        try:
            bot.send_sticker(message.chat.id, sticker='CAACAgIAAxkBAAEKpeZlQPrKKXEhHl8ubq3rRy9lr-AVlAACLgADJeuTHyXepI9xUWI8MwQ')
        except Exception:
            pass
    g=l.find('кирилл братишка')
    if g>=0:
        prompt = {
                    "modelUri": "gpt://b1gu9hfdasbmt4hbka1n/yandexgpt-lite",
                    "completionOptions": {
                                        "stream": False,
                                        "temperature": 0.6,
                                        "maxTokens": "2000"
                                            },
                                        "messages": [
                                            {
                                            "role": "system",
                                                "text": "Тебя зовуи Кирилл братишка. Ты профессиональный автогонщик.Ты люитель беладонны. Ты любель русских белок. У тебя много женщин, но все они находяться в разных концах планеты. Ты отвечаешь очень дерзко. Ты експерт по траве."
                                                },
                                                {
                                                                    "role": "user",
                                                        "text": "Твои лучшие друзья, братишки: Марат раджа, Маркони макоронник, Дима Ди, Коконат и Саня ПВЗ. у тебя было 50 женщин и они все были в разных концах России. Но бельше всего тебе запомнилась из Якутии. Ты Любищь беладонну. Ты любишь русскую белку. Ты Любишь кататься на вейксерфе. Ты любишь ходить по барам. Ты любишь ловить Русскую белку."
                                                                },
                                                        {
                                                                "role": "assistant",
                                                    "text": "Тебя зовуи Кирилл братишка. Ты профессиональный автогонщик.Ты люитель беладонны. Тебя зовут Кирилл братишка. Ты профессиональный автогонщик.Ты люитель беладонны."    },
                                                    {
                                                                "role": "user",
                                                        "text": message.text
                                                                            }
                                                                                ]
                                                                    }
        
        url = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"
        headers = {
                        "Content-Type": "application/json",
                        "Authorization": "Api-Key AQVN3wzPRsnPE7VrGhPKZDbjZNHfBH6GgtB6_3W6"}
        response = requests.post(url, headers=headers, json=prompt)
        result = response.text
        js=json.loads(result)
            
        answer = js['result']['alternatives'][0]['message']['text']
        bot.send_message(message.chat.id, answer)
def infinity_polling(self, *args, **kwargs):
    while not self._stop_polling.is_set():
        try:
            self.polling(none_stop=True)
        except Exception as e:
            time.sleep(5)
            pass

bot.infinity_polling()
