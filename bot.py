import os
import telebot
import json
from datetime import datetime
from telegram import Update
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext
from package.aggregation import aggregate_data

def start(update: Update, context: CallbackContext):
    """Send a message when the command /start is issued."""
    update.message.reply_text('Hi! Send me a JSON message with `start_date` and `end_date` fields.')

def handle_json(update: Update, context: CallbackContext):
    """Handle incoming JSON messages and extract start_date and end_date."""
    try:
        json_data = json.loads(update.message.text)
  
        data = json.loads(aggregate_data(json_data))

        update.message.reply_text(data)
    except (KeyError, ValueError):
        update.message.reply_text('Invalid JSON message format. Please include `start_date` and `end_date` fields in ISO format (e.g. "2022-04-06T12:00:00").')

def main():
    """Start the bot."""
    # Set up the Telegram bot
    updater = Updater(os.getenv('API_KEY'))
    dispatcher = updater.dispatcher

    # Define the command handlers
    start_handler = CommandHandler('start', start)
    dispatcher.add_handler(start_handler)

    # Define the message handler for JSON messages
    json_handler = MessageHandler(Filters.text & (~Filters.command), handle_json)
    dispatcher.add_handler(json_handler)

    # Start the bot
    updater.start_polling()
    updater.idle()


if __name__ == '__main__':
    main()
