import asyncio
import logging
import requests
import sqlite3
from datetime import datetime, timedelta, timezone
from telethon import TelegramClient
import click
from urllib.parse import urljoin
from currencyConfig import API_HASH, API_ID, CHAT_ID
import matplotlib.pyplot as plt

#Usage in cron or cmd: python3 currencyMonitor.py --monitor_currency_once
#Usage in cron or cmd: python3 currencyMonitor.py --send_historical_data_to_telegram

# Currency monitoring settings
CHECK_INTERVAL = 300  # 5 minutes

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Telegram client
client = TelegramClient('currency_monitor', API_ID, API_HASH)


class CurrencyDatabase:
    def __init__(self, db_name="currency_rates.db"):
        self.db_name = db_name
        self._create_table()

    def _create_table(self):
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS rates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT UNIQUE,
                    usd_rate REAL,
                    eur_rate REAL
                )
                """
            )
            conn.commit()

    def store_rates(self, usd_rate, eur_rate, date=None):
        if not date:
            date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO rates (date, usd_rate, eur_rate) VALUES (?, ?, ?)",
                (date, usd_rate, eur_rate)
            )
            conn.commit()

    def get_rates_for_specified_date(self, date=None):
        """Gets the rates for a specified date or yesterday if no date is provided."""
        if not date:
            date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT usd_rate, eur_rate FROM rates WHERE date = ?", (date,))
            return cursor.fetchone()  # Returns (usd_rate, eur_rate) or None
        
    def get_60_day_data(self):
        """Fetches the last 60 days of exchange rates."""
        end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        start_date = (datetime.now(timezone.utc) - timedelta(days=60)).strftime("%Y-%m-%d")
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT date, usd_rate, eur_rate
                FROM rates
                WHERE date BETWEEN ? AND ?
                ORDER BY date ASC
                """, (start_date, end_date)
            )
            return cursor.fetchall()  # Returns list of tuples (date, usd_rate, eur_rate)

    def get_60_day_average(self):
        """Calculates the 60-day average of USD and EUR rates."""
        end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        start_date = (datetime.now(timezone.utc) - timedelta(days=60)).strftime("%Y-%m-%d")
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT AVG(usd_rate), AVG(eur_rate)
                FROM rates
                WHERE date BETWEEN ? AND ?
                """, (start_date, end_date)
            )
            avg_usd_rate, avg_eur_rate = cursor.fetchone()
            # Round both averages to 2 decimal places
            return round(avg_usd_rate, 2), round(avg_eur_rate, 2)


    def populate_historic_data(self):
        """Fetches exchange rates for the last 60 days and stores them in the database."""
        logger.info("Fetching historic exchange rates for the last 60 days...")
        base_url = "https://www.cbr-xml-daily.ru"

        # Start with the latest available data
        current_url = f"{base_url}/daily_json.js"

        for _ in range(60):
            try:
                response = requests.get(current_url)
                if response.status_code != 200:
                    logger.warning(f"Failed to fetch {current_url}")
                    break

                data = response.json()
                date = datetime.strptime(data["Date"], "%Y-%m-%dT%H:%M:%S%z").strftime("%Y-%m-%d")
                usd_rate = data["Valute"]["USD"]["Value"]
                eur_rate = data["Valute"]["EUR"]["Value"]

                self.store_rates(usd_rate, eur_rate, date)
                logger.info(f"Stored: {date} | USD: {usd_rate}, EUR: {eur_rate}")

                # Move to previous date URL
                previous_url = data.get("PreviousURL")
                if not previous_url:
                    break

                # Proper URL handling
                current_url = urljoin(base_url, previous_url)

            except Exception as e:
                logger.error(f"Error fetching historical data: {e}")
                break


async def get_exchange_rates():
    try:
        response = requests.get("https://www.cbr-xml-daily.ru/daily_json.js")
        data = response.json()
        usd_rate = round(data["Valute"]["USD"]["Value"], 2)
        eur_rate = round(data["Valute"]["EUR"]["Value"], 2)
        return usd_rate, eur_rate
    except Exception as e:
        logger.error(f"Failed to fetch exchange rates: {e}")
        return None, None


async def send_telegram_alert(message):
    try:
        await client.send_message(CHAT_ID, message)
        logger.info(f"Alert sent: {message}")
    except Exception as e:
        logger.error(f"Failed to send Telegram message: {e}")

async def compare_with_average(db, usd_rate, eur_rate):
    """Compares current rates with 60-day average and sends alerts if there is a significant change."""
    avg_usd_rate, avg_eur_rate = db.get_60_day_average()
    
    if avg_usd_rate and avg_eur_rate:
        logger.info(f"60-day average - USD: {avg_usd_rate}, EUR: {avg_eur_rate}")

        # Build the message for USD
        usd_message = ""
        if usd_rate > avg_usd_rate:
            usd_message += f"📈 USD/RUB выше среднего.\nСреднее за 60 дней: {avg_usd_rate}.\nСегодня: {usd_rate}"
        elif usd_rate < avg_usd_rate:
            usd_message += f"📉 USD/RUB ниже среднего.\nСреднее за 60 дней: {avg_usd_rate}.\nСегодня: {usd_rate}"
        else:
            usd_message += f"➡️ USD/RUB без изменений: {usd_rate}"
        usd_message += "\n"

        # Build the message for EUR
        eur_message = ""
        if eur_rate > avg_eur_rate:
            eur_message += f"📈 EUR/RUB выше среднего.\nСреднее за 60 дней: {avg_eur_rate}.\nСегодня: {eur_rate}"
        elif eur_rate < avg_eur_rate:
            eur_message += f"📉 EUR/RUB ниже среднего.\nСреднее за 60 дней: {avg_eur_rate}.\nСегодня: {eur_rate}"
        else:
            eur_message += f"➡️ EUR/RUB без изменений: {eur_rate}"

        # Send the combined message for both USD and EUR
        message = f"{usd_message}\n{eur_message}"
        await send_telegram_alert(message)


async def monitor_currency(db):
    while True:
        usd_rate, eur_rate = await get_exchange_rates()
        if usd_rate and eur_rate:
            logger.info(f"USD/RUB: {usd_rate}, EUR/RUB: {eur_rate}")
            
            await compare_with_average(db, usd_rate, eur_rate)
            
            db.store_rates(usd_rate, eur_rate)

        await asyncio.sleep(CHECK_INTERVAL)

async def monitor_currency_once(db):
    """Monitor currency only once."""
    # Fetch exchange rates once
    usd_rate, eur_rate = await get_exchange_rates()
    if usd_rate and eur_rate:
        logger.info(f"USD/RUB: {usd_rate}, EUR/RUB: {eur_rate}")
        
        # Compare with the average rates (or any other processing logic)
        await compare_with_average(db, usd_rate, eur_rate)
        
        # Store the rates into the database
        db.store_rates(usd_rate, eur_rate)
    else:
        logger.warning("Failed to fetch exchange rates.")


async def housekeeping(days_old=1):
    delete_before_date = datetime.now(timezone.utc) - timedelta(days=days_old)
    logger.info(f"Deleting messages older than {delete_before_date}")
    try:
        async for message in client.iter_messages(CHAT_ID, offset_date=delete_before_date):
            await client.delete_messages(CHAT_ID, message.id)
        logger.info("Housekeeping completed.")
    except Exception as e:
        logger.error(f"Housekeeping error: {e}")

async def send_historical_data_to_telegram_async(db):
    """Send historical exchange rate data to Telegram as an image followed by a message."""
    data = db.get_60_day_data()
    logger.info(f"data: {data}")
    avg_usd_rate, avg_eur_rate = db.get_60_day_average()
    logger.info(f"avg_eur_rate: {avg_eur_rate}")
    today_rates = db.get_rates_for_specified_date()
    today_usd_rate, today_eur_rate = today_rates if today_rates else (None, None)

    if not data:
        logger.warning("No historical data available for the last 60 days.")
        return
    
    # If today's rates are not found, fetch exchange rates
    if today_usd_rate is None or today_eur_rate is None:
        logger.info("Today's rates not found in the database, fetching new exchange rates...")
        today_usd_rate, today_eur_rate = await get_exchange_rates()

    # 1. Plotting the graph
    dates = [entry[0] for entry in data]
    usd_rates = [entry[1] for entry in data]
    eur_rates = [entry[2] for entry in data]

    fig, ax = plt.subplots()
    ax.plot(dates, usd_rates, label="USD/RUB", color="blue")
    ax.plot(dates, eur_rates, label="EUR/RUB", color="green")
    ax.set_ylabel("Курс")
    ax.set_title("Курс обмена USD/RUB и EUR/RUB за последние 60 дней")
    ax.legend()
    # Modify X-axis labels: show every 7th date
    ax.set_xticks(dates[::7])
    ax.set_xticklabels(dates[::7], rotation=45)
    plt.tight_layout()

    # 2. Save the plot to a file
    file_path = f"/tmp/exchange_rates_{datetime.now().strftime('%Y%m%d%H%M%S')}.jpg"
    plt.savefig(file_path, format='jpg', dpi=100)
    plt.close(fig)  # Close the figure to free memory

    # 3. Prepare the caption with exchange rate data
    caption = "**Сравнение среднего курса с сегодняшним за последние 60 дней:**\n\n"
    caption += "```"
    caption += "Валюта   | Средний | Сегодняшний\n"
    caption += "------------------------------------\n"
    caption += f"  USD    | {avg_usd_rate:.2f}   | {today_usd_rate if today_usd_rate else 'N/A'}\n"
    caption += f"  EUR    | {avg_eur_rate:.2f}   | {today_eur_rate if today_eur_rate else 'N/A'}\n"
    caption += "```\n"

    # 3. Send the image file to Telegram
    try:
        entity = await client.get_entity(CHAT_ID)
        await client.send_file(entity, file_path, caption=caption, parse_mode="markdown")
        logger.info("Sent historical data graph to Telegram.")
    except Exception as e:
        logger.error(f"Failed to send historical data graph: {e}")

#Usage: currencyMonitor.py --send_historical_data_to_telegram
#Usage 2: currencyMonitor.py --housekeeping --days 0

@click.command()
@click.option('--housekeeping', is_flag=True, help="Clean up old messages in the chat.")
@click.option('--days', default=1, type=int, help="Number of days for housekeeping. Default is 1.")
@click.option('--get_historic_data', is_flag=True, help="Populate SQLite with the last 60 days of exchange rates.")
@click.option('--send_historical_data_to_telegram', is_flag=True, help="Send historical data to Telegram.")
@click.option('--monitor_currency_once', is_flag=True, help="Monitor currency only once.")
def main(housekeeping, days, get_historic_data, send_historical_data_to_telegram, monitor_currency_once):
    """CLI entry point"""
    asyncio.run(run_async_tasks(housekeeping, days, get_historic_data, send_historical_data_to_telegram, monitor_currency_once))


async def run_async_tasks(housekeeping_flag, days, get_historic_data_flag, send_historical_data_to_telegram_flag, monitor_currency_once_flag):
    """Runs the Telegram bot tasks."""
    async with client:
        db = CurrencyDatabase()

        if get_historic_data_flag:
            db.populate_historic_data()
        elif send_historical_data_to_telegram_flag:
            await send_historical_data_to_telegram_async(db)
        elif housekeeping_flag:
            await housekeeping(days)
        elif monitor_currency_once_flag:
            # Monitor currency once (only one cycle)
            await monitor_currency_once(db)
        else:
            # Default behavior: continuously monitor currency
            await monitor_currency(db)


if __name__ == "__main__":
    main()