import asyncio
import concurrent
import json
import logging
import random
import re
from concurrent.futures import ThreadPoolExecutor
import redis.asyncio as aioredis
import aiohttp
import pandas
import redis
import requests
from aiohttp import ClientSession, ClientTimeout
from bs4 import BeautifulSoup

# LOGGING
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="logs.log",
    filemode="a",
)


BASE_URL = "https://yacht-parts.ru"
URL = "https://yacht-parts.ru/catalog/"


async def fetch(session, url, retries=10, delay=20, headers=None):
    """Функция для выполнения HTTP GET запроса."""
    if isinstance(url, bytes):
        url = url.decode("utf-8")

    headers = headers or {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36 OPR/38.0.2220.41"
    }

    try:
        async with session.get(url, headers=headers) as response:
            response.raise_for_status()
            return await response.text()

    except (
        aiohttp.ClientError,
        asyncio.TimeoutError,
        aiohttp.ClientResponseError,
    ) as exc:
        if exc.status == 404:
            logging.warning(f"404 Error: URL not found. Skipping...")
            return None
        if retries > 0:
            random_delay = delay + random.uniform(10, 30)  # Рандомная задержка
            logging.warning(
                f"Error {type(exc).__name__}: {str(exc)} while fetching {url}. "
                f"Retrying in {random_delay:.2f} seconds (retries left: {retries})."
            )
            await asyncio.sleep(random_delay)
            return await fetch(
                session, url, retries - 1, delay + random.uniform(2, 10), headers
            )
        else:
            logging.error(f"MAX RETRIES EXCEEDED FOR URL: {url}!")
            raise

    except Exception as exc:
        logging.error(f"An unexpected error occurred while fetching {url}: {exc}")
        raise


async def process_categories(redis):
    """Обрабатывает категории с заданного URL и добавляет ссылки на категории в Redis."""
    async with ClientSession() as session:
        response_text = await fetch(session, URL)
        soup = BeautifulSoup(response_text, "lxml")
        category_response = soup.find_all("li", class_="sect")
        for category in category_response:
            for link in category.find_all("a", href=True):
                new_link = BASE_URL + link["href"]
                await redis.sadd("category_links_set", new_link)


async def get_last_page_number(session, url):
    """Функция для получения номера последней страницы из ответа сервера."""
    response_text = await fetch(session, url)
    soup = BeautifulSoup(response_text, "lxml")
    pages = soup.find_all("span", class_="nums")
    if pages:
        max_page = pages[0].find_all("a")
        if max_page:
            return int(max_page[-1].text)

    return 1


async def fetch_product_and_store(redis, product_url):
    """Функция для добавления ссылки на продукт в Redis и логирования."""
    new_link = BASE_URL + product_url
    await redis.sadd("product_links_set", new_link)

    count = await redis.scard("product_links_set")
    logging.info(f"Add new product link: {new_link[30:]}. Total REDIS count: {count}.")


async def fetch_product(semaphore, session, redis, category_link, page):
    """Функция для извлечения продуктов с заданной страницы категории."""
    async with semaphore:
        response_text = await fetch(session, f"{category_link}?PAGEN_1={page}")
        soup = BeautifulSoup(response_text, "lxml")
        products = soup.find_all("div", class_="list_item_wrapp item_wrap")

        for product in products:
            product_url = product.find("a", href=True)["href"]

            await fetch_product_and_store(redis, product_url)
        logging.info(f"Finished processing page {page} from {category_link[30:]}.")


async def worker_links(queue, semaphore, session, redis):
    """Процесс для извлечения продуктов из очереди категорий."""
    while True:
        category_link, page = await queue.get()
        if category_link is None:
            break

        await fetch_product(semaphore, session, redis, category_link, page)
        queue.task_done()
        logging.info(
            f"Task DONE for page {page} {category_link[30:]}. Tasks remaining in queue: {queue.qsize()}."
        )


async def fetch_category_data(redis, session, category_link, semaphore):
    """Функция для получения данных о категориях и обработки всех страниц."""
    last_page_number = await get_last_page_number(session, category_link)
    queue = asyncio.Queue()
    workers = [
        asyncio.create_task(worker_links(queue, semaphore, session, redis))
        for _ in range(40)
    ]
    for page in range(1, last_page_number + 1):
        await queue.put((category_link, page))
        logging.info(
            f"Added page {page} / {last_page_number} for ...{category_link[30:]} to the queue."
        )
    await queue.join()
    for w in workers:
        await queue.put((None, None))

    await asyncio.gather(*workers)


async def process_all_categories_links(redis, semaphore):
    """Функция для обработки всех ссылок категорий и получения данных о продуктах."""
    async with ClientSession() as session:
        category_links = await redis.smembers("category_links_set")
        category_links = [link.decode("utf-8") for link in category_links]

        tasks = [
            fetch_category_data(redis, session, link, semaphore)
            for link in category_links
        ]
        await asyncio.gather(*tasks)


async def fetch_and_store_product_data(session, product_url, redis):
    """Функция для извлечения данных о продукте с заданного URL."""

    response_text = await fetch(session, product_url)
    soup = BeautifulSoup(response_text, "html.parser")

    # description
    preview_text_div = soup.find("div", class_="preview_text")
    description = preview_text_div.text.strip() if preview_text_div else None

    # brandname
    brandname_div = soup.find("div", class_="brand iblock")
    brandname = (
        brandname_div.find("a").find("img").get("title") if brandname_div else None
    )

    # imgs
    img_div = soup.find("div", class_="slides")
    imgs = (
        [BASE_URL + a.get("href") for a in img_div.find_all("a")] if img_div else None
    )

    # article
    article_div = soup.find("div", class_="article iblock")
    article = article_div.find("span", class_="value").text if article_div else None

    # name
    name_div = soup.find("h1", id="pagetitle")
    name_title = name_div.text if name_div else None

    # price
    price_div = soup.find("div", class_="price")
    price_num = price_div.text.strip() if price_div else "Под заказ"

    # category
    breadcrumbs_div = soup.find("div", id="navigation").find_all(itemprop="name")
    category_parts = (
        f"{breadcrumbs_div[2].text.strip()}/{breadcrumbs_div[3].text.strip()}"
        if breadcrumbs_div
        else None
    )

    # script
    scripts = soup.find_all("script")
    for script in scripts:
        if "agec_detail_script" in str(script):
            json_match = re.search(
                r"var agec_detail_base = (\{.*?\});", str(script), re.DOTALL
            )
            if json_match:
                json_text = json_match.group(1)
                json_data = json.loads(json_text)
                products = json_data["ecommerce"]["detail"]["products"]
                for product in products:
                    product_data = {
                        "category": product.get("category", category_parts),
                        "article": product.get("variant", article),
                        "brandname": brandname,
                        "name": product.get("name", name_title),
                        "price": product.get("price", price_num),
                        "description": description,
                        "imgs": imgs,
                    }

                    product_data_json = json.dumps(product_data)
                    await redis.rpush("products_data", product_data_json)
                    count = await redis.llen("products_data")
                    logging.info(f"Add product data: {article}. REDIS TOTAL: {count}.")

    # default
    product_data = {
        "category": category_parts,
        "article": article,
        "brandname": brandname,
        "name": name_title,
        "price": price_num,
        "description": description,
        "imgs": imgs,
    }

    product_data_json = json.dumps(product_data)
    await redis.rpush("products_data", product_data_json)
    count = await redis.llen("products_data")
    logging.info(f"Add product data: {article}. REDIS TOTAL: {count}.")


async def worker_product(queue, session, redis, semaphore):
    """Процесс для извлечения атрибутов продуктов из очереди продуктов."""
    while True:
        product_url = await queue.get()
        if product_url is None:
            break
        async with semaphore:
            await fetch_and_store_product_data(session, product_url, redis)
            logging.info(f"CURRENT QUEUE SIZE: {queue.qsize()}")
        queue.task_done()


async def fetch_all_products_data(redis, product_links, semaphore):
    """Асинхронная функция для обработки всех ссылок на продукты и извлечения данных."""
    queue = asyncio.Queue()

    async with ClientSession() as session:
        workers = [
            asyncio.create_task(worker_product(queue, session, redis, semaphore))
            for _ in range(70)
        ]

        for product_url in product_links:
            await queue.put(product_url)

        await queue.join()

        for _ in workers:
            await queue.put(None)
        await asyncio.gather(*workers)

    all_products_data = []
    product_data_list = await redis.lrange("products_data", 0, -1)

    for product_data_json in product_data_list:
        product_data = json.loads(product_data_json)
        all_products_data.append(product_data)

    return all_products_data


async def main():
    redis = await aioredis.from_url("redis://localhost:6379")
    semaphore = asyncio.Semaphore(160)
    try:

        # await process_categories(redis)

        # await process_all_categories_links(redis, semaphore)

        # product_links = await redis.smembers("product_links_set")
        # product_links = [link.decode("utf-8") for link in product_links]
        # logging.info(f"STATISTIC Total product links: {len(product_links)}")

        products_data = await redis.lrange("products_data", 0, -1)

        products = []

        counter = 1

        for product_json in products_data:
            product = json.loads(product_json)
            products.append(product)
            logging.info(f"Succes append. TOTAL: {counter}")
            counter += 1

        dataframe = pandas.DataFrame(products)

        excel_filename = "products_data.xlsx"
        dataframe.to_excel(excel_filename, index=False)
        logging.info(f"OPERATION COMPLITE! EXCEL: {excel_filename}")

    finally:
        await redis.aclose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
