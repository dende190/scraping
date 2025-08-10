import asyncio
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

async def main():
  url = 'https://quotes.toscrape.com/scroll';
  scrollWaitInMiliseconds = 2000;

  async with async_playwright() as playwrightAsync:
    browser = await playwrightAsync.chromium.launch(headless=False)
    context = await browser.new_context()
    page = await context.new_page()

    await page.goto(url)
    # await page.wait_for_timeout(3000)
    # html = await page.content()
    # await browser.close()

    # pythonHtml = BeautifulSoup(html, 'html.parser')
    # quotes = pythonHtml.select('div.quote')

    # print('Citas encotnradas', len(quotes))

    # for quote in quotes:
    #   text = quote.find('span', class_='text').get_text()
    #   author = quote.find('small', class_='author').get_text()
    #   print(f'{text} - {author}')

    # await browser.close()

    # print(f'Frases cargadas', len(quotes))
    # for quote in quotes:
    #   print(quote)

#-------------------------------------------------------------------------------


    # await page.click('.button.main-button')
    # await page.wait_for_timeout(6000)
    # await browser.close()


#-------------------------------------------------------------------------------


    quotes = set();
    lastHight = await page.evaluate('document.body.scrollHeight')

    for iteration in range(3):
      await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
      await page.wait_for_timeout(scrollWaitInMiliseconds)

      dQuotes = await page.query_selector_all('.quote')
      for dQuote in dQuotes:
        dQuoteText = await dQuote.query_selector('.text')
        if dQuoteText:
          text = await dQuoteText.inner_text()
          quotes.add(text)

      newHeight = await page.evaluate('document.body.scrollHeight')
      if newHeight == lastHight:
        break

      lastHight = newHeight

    await browser.close()

    print(f'Frases cargadas', len(quotes))
    for quote in quotes:
      print(quote)

asyncio.run(main())
