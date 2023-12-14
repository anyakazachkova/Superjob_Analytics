import pandas as pd
import numpy as np

from telegram.ext import Updater, CommandHandler, MessageHandler, Filters


class SuperjobAnalyticsBot:

    def __init__(self, 
                 bot_key: str, 
                 mode: str
                 ):

        self.mode = mode

        self.updater = Updater(bot_key, use_context=True)
        self.dispatcher = self.updater.dispatcher

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

        if mode == 'dev':
            self.data = pd.read_parquet(
                'results/parsed_data'
            )

    def run(self):
        self.updater.start_polling()
        self.updater.idle()        

    @staticmethod
    def start(update, context):
        answer = "Start\n"
        answer += "/start - Start the bot\n"
        answer += "/vacancies_count - Show amount of vacancies\n"
        answer += "/salary_stat - Show current salary statistics"
        update.message.reply_text(answer)

    def vacancies_count(self, update, context):
        result = self.data['name'].count()
        update.message.reply_text(f'Currently there are {result} vacancies')

    def salary_stat(self, update, context):
        self.data['salary'] = self.data['salary'].apply(
            lambda x: int(x) 
            if len(x) > 0 else np.nan
        )
        result = self.data['salary'].mean()
        update.message.reply_text(f'Mean salary is {result}')

    @staticmethod
    def echo(update, context):
        update.message.reply_text('You said: ' + update.message.text)