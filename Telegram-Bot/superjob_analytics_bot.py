import os
import pandas as pd
import numpy as np

from telegram.ext import Updater, CommandHandler, MessageHandler, Filters


class SuperjobAnalyticsBot:

    def __init__(self, 
                 bot_key: str
                 ):

        self.updater = Updater(bot_key, use_context=True)
        self.dispatcher = self.updater.dispatcher
        self.metrics = self.load_calculated_metrics()

        start_handler = CommandHandler('start', self.start)
        vacancies_count_handler = CommandHandler(
            'vacancies_count', 
            self.vacancies_count
        )
        salary_stat_handler = CommandHandler(
            'salary_stat', 
            self.salary_stat
        )
        echo_handler = MessageHandler(
            Filters.text & ~Filters.command, 
            self.echo
        )
    

        self.dispatcher.add_handler(start_handler)
        self.dispatcher.add_handler(vacancies_count_handler)
        self.dispatcher.add_handler(salary_stat_handler)
        self.dispatcher.add_handler(echo_handler)

    def load_calculated_metrics(self):
        with open('/home/anyakazachkova/Superjob_Parser/results/superjob_metrics.csv', 
                  'r', encoding='utf-8') as file:
            csv_data = file.read()

        rows = csv_data.strip().split('\n')
        cleaned_data = [row.strip('|').split('|') for row in rows]
        cleaned_data = [
            [value.strip() for value in row] 
            for row in cleaned_data
        ]
        cleaned_data = [el for el in cleaned_data if len(el) > 1]

        self.metrics = pd.DataFrame(
            cleaned_data[1:], columns=cleaned_data[0]
        )

        return self.metrics

    def run(self):
        self.updater.start_polling()
        self.updater.idle()        

    def vacancies_count(self, update, context):
        result = self.metrics['superjob_metrics.n_count'].sum()
        update.message.reply_text(
            f"Currently there are total {result} vacancies"
        )

    def salary_stat(self, update, context):
        result = self.metrics.to_dict()
        update.message.reply_text(f'{result}')
    
    @staticmethod
    def start(update, context):
        answer = "Start\n"
        answer += "/start - Start the bot\n"
        answer += "/vacancies_count - Show amount of vacancies\n"
        answer += "/salary_stat - Show current salary statistics"
        update.message.reply_text(answer)

    @staticmethod
    def echo(update, context):
        update.message.reply_text('You said: ' + update.message.text)