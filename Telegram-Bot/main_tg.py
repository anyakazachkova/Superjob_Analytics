from superjob_analytics_bot import SuperjobAnalyticsBot

from keys import BOT_KEY


def main():
    bot = SuperjobAnalyticsBot(
        bot_key=BOT_KEY
    )
    bot.run()

if __name__ == '__main__':
    main()