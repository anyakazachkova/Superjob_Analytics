from telegram.ext import Updater, CommandHandler, MessageHandler, Filters
import urllib3
import requests

from keys import BOT_KEY

def start(update, context):
    update.message.reply_text('Start')

def echo(update, context):
    update.message.reply_text('You said: ' + update.message.text)

def main():
    updater = Updater(BOT_KEY, use_context=True)
    dispatcher = updater.dispatcher

    start_handler = CommandHandler('start', start)
    dispatcher.add_handler(start_handler)

    echo_handler = MessageHandler(Filters.text & ~Filters.command, echo)
    dispatcher.add_handler(echo_handler)

    updater.start_polling()
    updater.idle()

if __name__ == '__main__':
    main()