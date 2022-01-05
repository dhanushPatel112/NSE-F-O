#!/usr/bin/env python
# -*- coding: utf-8 -*-
# This program is dedicated to the public domain under the CC0 license.

"""
Simple Bot to reply to Telegram messages.

First, a few handler functions are defined. Then, those functions are passed to
the Dispatcher and registered at their respective places.
Then, the bot is started and runs until we press Ctrl-C on the command line.

Usage:
Basic Echobot example, repeats messages.
Press Ctrl-C on the command line or send a signal to the process to stop the
bot.
"""
import logging
import io
import csv
import os
try:
    from nsepython import *
    import pandas as pd
    from datetime import datetime
    from dateutil.relativedelta import relativedelta, TH
except:
    import pip
    def install(package):
        if hasattr(pip, 'main'):
            pip.main(['install', package])
        else:
            pip._internal.main(['install', package])

    install("nsepython")
    install("pandas")
    install("datetime")
    from nsepython import *
    import pandas as pd
    from datetime import datetime
    from dateutil.relativedelta import relativedelta, TH

from telegram.ext import Updater, CommandHandler, MessageHandler, Filters

# Enable logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

logger = logging.getLogger(__name__)

PORT = int(os.environ.get('PORT', '8443'))

# Define a few command handlers. These usually take the two arguments update and
# context. Error handlers also receive the raised TelegramError object in error.


def start(update, context):
    """Send a message when the command /start is issued."""
    update.message.reply_text('Starting fetching process. This cloud take few minutes. Please wait...')

    # Here i  have to do my stuff and store result in test_data

    list_of_company = fnolist()
    list_of_company = sorted(list_of_company[3:])

    lot_size_df = pd.read_csv("https://www1.nseindia.com/content/fo/fo_mktlots.csv", sep="\s*,\s*", engine="python")

    def get_lot(company, expiry_date):
        j = 0
        for symbol in lot_size_df['SYMBOL']:
            if symbol == company:
                # lot = lot_size_df['NOV-20'][j]
                lot = lot_size_df[expiry_date][j]
                break
            j += 1
        return str(lot)

    current_month = 2
    next_month = 3

    y = int(lot_size_df.columns.values[current_month].split("-")[1]) + 2000
    m = datetime.strptime(lot_size_df.columns.values[current_month].split("-")[0], "%b").month
    m_text = datetime.strptime(lot_size_df.columns.values[current_month].split("-")[0], "%b").strftime("%b")
    d = 1
    date_th = (datetime(y, m, d) + relativedelta(day=31,
               weekday=TH(-1))).strftime("%d")

    combined_company_df = pd.DataFrame(columns=['CALLS_OI', 'CALLS_Chng in OI', 'CALLS_Volume', 'CALLS_IV', 'CALLS_LTP',
                                                'CALLS_Net Chng', 'Strike Price', 'PUTS_OI', 'PUTS_Chng in OI',
                                                'PUTS_Volume', 'PUTS_IV', 'PUTS_LTP', 'PUTS_Net Chng',
                                                'CALLS_Ask Price', 'CALLS_Ask Qty', 'CALLS_Bid Price', 'CALLS_Bid Qty',
                                                'PUTS_Ask Price', 'PUTS_Ask Qty', 'PUTS_Bid Price', 'PUTS_Bid Qty'])

    for company in list_of_company:
        try:
            # 27-Jan-2020
            oi_data, ltp, crontime = oi_chain_builder(
                company, str(date_th)+"-"+m_text+"-"+str(y), "full")
            oi_data['Company_name'] = company
            oi_data['ltp'] = ltp
            oi_data['lot_size'] = get_lot(company, str(
                lot_size_df.columns.values[current_month]))
            combined_company_df = combined_company_df.append(
                oi_data, ignore_index=True)
        except Exception as e:
            update.message.reply_text("error in " + company)
            update.message.reply_text("error : ", e)
            pass

    test_data = combined_company_df.values.tolist()
    test_data.insert(0, ['CALLS_OI', 'CALLS_Chng in OI', 'CALLS_Volume', 'CALLS_IV', 'CALLS_LTP', 'CALLS_Net Chng', 'Strike Price', 'PUTS_OI', 'PUTS_Chng in OI', 'PUTS_Volume', 'PUTS_IV', 'PUTS_LTP', 'PUTS_Net Chng',
                     'CALLS_Ask Price', 'CALLS_Ask Qty', 'CALLS_Bid Price', 'CALLS_Bid Qty', 'PUTS_Ask Price', 'PUTS_Ask Qty', 'PUTS_Bid Price', 'PUTS_Bid Qty', 'CALLS_Chart', 'PUTS_Chart', 'Company_name', 'ltp', 'lot_size'])

    # csv module can write data in io.StringIO buffer only
    s = io.StringIO()
    csv.writer(s).writerows(test_data)
    s.seek(0)

    # python-telegram-bot library can send files only from io.BytesIO buffer
    # we need to convert StringIO to BytesIO
    buf = io.BytesIO()

    # extract csv-string, convert it to bytes and write to buffer
    buf.write(s.getvalue().encode())
    buf.seek(0)

    # set a filename with file's extension
    buf.name = str(date_th) + "_" + str(m) + '_current_month.csv'

    # send the buffer as a regular file
    context.bot.send_document(chat_id=update.message.chat_id, document=buf)
    update.message.reply_text('Done!')


def help(update, context):
    """Send a message when the command /help is issued."""
    update.message.reply_text('I will send nse option chain data of all company with message /start')


def echo(update, context):
    """Echo the user message."""
    update.message.reply_text("I'm sorry, I don't understand that command. Please type /start to get started.")


def error(update, context):
    """Log Errors caused by Updates."""
    logger.warning('Update "%s" caused error "%s"', update, context.error)


def main():
    """Start the bot."""
    # Create the Updater and pass it your bot's token.
    # Make sure to set use_context=True to use the new context based callbacks
    # Post version 12 this will no longer be necessary
    updater = Updater(
        "5050768979:AAHwB9EBSpsfJNSZu88dCyTuvmZPjuKDuwo", use_context=True)

    # Get the dispatcher to register handlers
    dp = updater.dispatcher

    # on different commands - answer in Telegram
    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CommandHandler("help", help))

    # on noncommand i.e message - echo the message on Telegram
    dp.add_handler(MessageHandler(Filters.text, echo))

    # log all errors
    dp.add_error_handler(error)

    # Start the Bot
    updater.start_webhook(listen="0.0.0.0",
                          port=PORT,
                          url_path="5050768979:AAHwB9EBSpsfJNSZu88dCyTuvmZPjuKDuwo")
    updater.bot.setWebhook('https://nse-option-chain-dhanush.herokuapp.com/' + "5050768979:AAHwB9EBSpsfJNSZu88dCyTuvmZPjuKDuwo")
    
    # updater.start_polling()

    # Run the bot until you press Ctrl-C or the process receives SIGINT,
    # SIGTERM or SIGABRT. This should be used most of the time, since
    # start_polling() is non-blocking and will stop the bot gracefully.
    updater.idle()


if __name__ == '__main__':
    main()
