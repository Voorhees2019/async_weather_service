import asyncio
import json
from datetime import datetime

import aiosqlite
from aiohttp import ClientSession, web
from aiologger.loggers.json import JsonLogger

logger = JsonLogger.with_default_handlers(
    level='DEBUG',
    serializer_kwargs={'ensure_ascii': False},
)

DB_NAME = 'weather.db'
app_storage = {}


async def db_create_table():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('CREATE TABLE IF NOT EXISTS requests '
                         '(date text, city text, weather text)')
        await db.commit()


async def db_insert(city: str, weather: str):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('INSERT INTO requests VALUES (?, ?, ?)',
                         (datetime.now(), city, weather))
        await db.commit()


async def get_weather(city: str):
    url = f'http://api.openweathermap.org/data/2.5/weather'
    params = {'q': city, 'APPID': '2a4ff86f9aaa70041ec8e82db64abf56'}

    async with app_storage['session_openweathermap'].get(url=url, params=params) as response:
        weather_json = await response.json()

        try:
            return weather_json['weather'][0]['main']
        except KeyError:
            return 'No data available'


async def get_translation(text: str, source_lang: str = 'auto', target_lang: str = 'en'):
    # await logger.info(f'Translate: "{text}"')

    url = 'https://libretranslate.de/translate'
    data = {'q': text, 'source': source_lang,
            'target': target_lang, 'format': 'text'}

    async with app_storage['session_libretranslate'].post(url=url, json=data) as response:
        translation_json = await response.json()

        try:
            return translation_json['translatedText']
        except KeyError:
            logger.error(
                f'Unable to translate "{text}". {translation_json.get("error", None)}')
            return text


async def handle(request):
    city = request.rel_url.query['city']

    # await logger.info(f'Request weather status in "{city}"')

    city_en = await get_translation(city)
    weather = await get_weather(city_en)

    await db_insert(city_en, weather)

    response = {'city': city, 'weather': weather}
    return web.Response(text=json.dumps(response, ensure_ascii=False), status=200)


async def main():
    app_storage['session_openweathermap'] = ClientSession()
    app_storage['session_libretranslate'] = ClientSession()

    await db_create_table()

    app = web.Application()
    app.add_routes([web.get('/weather', handle)])

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, 'localhost', 8080)
    await site.start()

    while True:
        await asyncio.sleep(3600)


if __name__ == '__main__':
    asyncio.run(main())
