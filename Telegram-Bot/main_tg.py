from superjob_analytics_bot import SuperjobAnalyticsBot

from keys import BOT_KEY

MODE = 'dev'

def main():
    bot = SuperjobAnalyticsBot(
        bot_key=BOT_KEY,
        mode=MODE
    )
    bot.run()

if __name__ == '__main__':
    main()