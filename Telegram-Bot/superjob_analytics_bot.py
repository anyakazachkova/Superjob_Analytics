from telegram.ext import Updater, CommandHandler, MessageHandler, Filters


class SuperjobAnalyticsBot:

    def __init__(self, bot_key):

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

    @staticmethod
    def vacancies_count(update, context):
        update.message.reply_text('Here will be vacancies count')

    @staticmethod
    def salary_stat(update, context):
        update.message.reply_text('Here will be salary stat')

    @staticmethod
    def echo(update, context):
        update.message.reply_text('You said: ' + update.message.text)