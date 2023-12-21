import os
import io
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from prettytable import PrettyTable

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
            self.command_vacancies_count
        )
        salary_stat_handler = CommandHandler(
            'salary_stat', 
            self.command_salary_stat
        )
        echo_handler = MessageHandler(
            Filters.text & ~Filters.command, 
            self.command_echo
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

        metrics = pd.DataFrame(
            cleaned_data[1:], 
            columns=cleaned_data[0]
        )

        metrics.columns = [
            col.split('.')[1]
            for col in metrics.columns
        ]

        metrics.iloc[:, 1:] = metrics.iloc[:, 1:] \
                                .replace('NULL', np.nan) \
                                .astype(float) \
                                .round(2)
        metrics['n_count'] = metrics['n_count'] \
                                .astype(int)
        self.metrics = metrics

        return self.metrics

    def run(self):
        self.updater.start_polling()
        self.updater.idle()

    def send_image(self, update, context, image):
        bio = io.BytesIO()
        image.savefig(bio, format='png')
        bio.seek(0)
        context.bot.send_photo(
            update.message.chat_id, photo=bio
        )

    def command_vacancies_count(self, update, context):
        
        result = self.metrics['n_count'].sum()
        update.message.reply_text(
            f"Currently there are {int(result)} vacancies in total"
        )

        table = self.plot_table(
            df=self.metrics[['keyword', 'n_count']]
        )
        self.send_image(
            update, 
            context, 
            table
        )

        plot = self.plot_metrics(
            df=self.metrics,
            col='n_count'
        )
        self.send_image(
            update, 
            context, 
            plot
        )

    def command_salary_stat(self, update, context):

        update.message.reply_text(
            'Here is current salary statistics'
        )        

        table = self.plot_table(
            df=self.metrics
        )
        self.send_image(
            update, 
            context, 
            table
        )

        for el in ['mean', 'median', 'min', 'max']:
            salary_plot = self.plot_metrics(
                self.metrics,
                f'{el}_salary'
            )
            self.send_image(
                update, 
                context, 
                salary_plot
            )
    
    @staticmethod
    def start(update, context):
        answer = "Start\n"
        answer += "/start - Start the bot\n"
        answer += "/vacancies_count - Show amount of vacancies\n"
        answer += "/salary_stat - Show current salary statistics"
        update.message.reply_text(answer)

    @staticmethod
    def command_echo(update, context):
        update.message.reply_text('You said: ' + update.message.text)

    @staticmethod
    def plot_metrics(df, col: str):
        plt.figure(figsize=(10, 6))
        sns.barplot(
            x=col,
            y='keyword',
            data=df.dropna(), 
            palette='viridis'
        )
        plt.title(f'{col} by keyword')
        plt.gcf().patch.set_facecolor('none')
        plt.gca().set_axisbelow(True)
        plt.grid(
            color='gray', 
            linestyle='-', 
            linewidth=0.25, 
            alpha=0.5
        )
        plt.tight_layout()
        return plt
    
    @staticmethod
    def plot_table(df):
        _, ax = plt.subplots(
            figsize=(12, 3)
        )
        table = ax.table(
            cellText=df.values, 
            colLabels=df.columns, 
            loc='center'
        )
        table.scale(1.4, 1.4)
        ax.axis('off')
        plt.tight_layout()
        return plt