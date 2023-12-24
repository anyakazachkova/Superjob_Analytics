import os
import io
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

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
        salary_stat_by_keyword_handler = CommandHandler(
            'salary_stat_by_keyword', 
            self.command_salary_stat_by_keyword
        )
        salary_stat_by_experience_handler = CommandHandler(
            'salary_stat_by_experience', 
            self.command_salary_stat_by_experience
        )
        salary_stat_by_employment_handler = CommandHandler(
            'salary_stat_by_employment', 
            self.command_salary_stat_by_employment
        )
        echo_handler = MessageHandler(
            Filters.text & ~Filters.command, 
            self.command_echo
        )
    

        self.dispatcher.add_handler(start_handler)
        self.dispatcher.add_handler(vacancies_count_handler)
        self.dispatcher.add_handler(salary_stat_by_keyword_handler)
        self.dispatcher.add_handler(salary_stat_by_experience_handler)
        self.dispatcher.add_handler(salary_stat_by_employment_handler)
        self.dispatcher.add_handler(echo_handler)

    def load_calculated_metrics(self):
        with open('results/superjob_metrics.csv', 
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

        metrics.iloc[:, 2:] = metrics.iloc[:, 2:] \
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
            df=self.metrics[
                ['group_name', 'group_type',
                 'n_count'
                 ]
            ]
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

    def command_salary_stat_by_keyword(self, update, context):
        self.salary_stat(
            update,
            context,
            group_type='keyword'
        )

    def command_salary_stat_by_experience(self, update, context):
        self.salary_stat(
            update,
            context,
            group_type='experience'
        )

    def command_salary_stat_by_employment(self, update, context):
        self.salary_stat(
            update,
            context,
            group_type='employment'
        )

    def salary_stat(self, update, context, group_type):

        update.message.reply_text(
            'Here is current salary statistics'
        )        

        table = self.plot_table(
            df=self.metrics,
            group_type=group_type
        )
        self.send_image(
            update, 
            context, 
            table
        )

        for el in ['mean', 'median', 'min', 'max']:
            salary_plot = self.plot_metrics(
                self.metrics,
                f'{el}_salary',
                group_type=group_type
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
        answer += "/salary_stat_by_keyword - Show current salary statistics by keyword\n"
        answer += "/salary_stat_by_experience - Show current salary statistics by experience\n"
        answer += "/salary_stat_by_employment - Show current salary statistics by employment\n"
        update.message.reply_text(answer)

    @staticmethod
    def command_echo(update, context):
        update.message.reply_text('You said: ' + update.message.text)

    @staticmethod
    def plot_metrics(df, 
                     col: str,
                     group_type: str = 'keyword'
                     ):
        data = df.loc[
            df['group_type'] == group_type
        ]
        
        plt.figure(figsize=(10, 6))
        sns.barplot(
            x=col,
            y='group_name',
            data=data.dropna(), 
            palette='viridis'
        )
        plt.title(f'{col} by {group_type}')
        plt.gcf().patch.set_facecolor('none')
        plt.gca().set_axisbelow(True)
        plt.xlabel(
            f'{col}, RUB per month' 
            if col != 'n_count' else f'{col}'
        )
        plt.grid(
            color='gray', 
            linestyle='-', 
            linewidth=0.25, 
            alpha=0.5
        )
        plt.tight_layout()
        return plt
    
    @staticmethod
    def plot_table(df,
                   group_type: str = 'keyword'
                   ):
        data = df.loc[
            df['group_type'] == group_type
        ]
        _, ax = plt.subplots(
            figsize=(12, 3)
        )
        table = ax.table(
            cellText=data.values, 
            colLabels=data.columns, 
            loc='center'
        )
        table.scale(1.4, 1.4)
        ax.axis('off')
        plt.tight_layout()
        return plt