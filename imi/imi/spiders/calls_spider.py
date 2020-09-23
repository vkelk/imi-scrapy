import logging
import re
import scrapy
from scrapy.loader import ItemLoader
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from imi.items import CallItem


logger = logging.getLogger()


class CallsSpider(CrawlSpider):
    name = 'calls'
    allowed_domains = ['imi.europa.eu']
    base_url = 'https://imi.europa.eu/'
    start_urls = [
        'https://www.imi.europa.eu/apply-funding/open-calls',
        'https://www.imi.europa.eu/apply-funding/closed-calls'
    ]
    # rules = [Rule(LinkExtractor(allow='catalogue/'), callback='parse_filter_book', follow=True)]
    rules = [
        Rule(LinkExtractor(
            # unique=True,
            allow=(r'apply-funding\/\w+-calls\/[A-Za-z0-9-]+'),
            # restrict_xpaths=('//article')
            ),
            callback='parse_item', follow=True)
    ]

    def parse_item(self, response):
        i = CallItem()
        i['call_id'] = response.xpath('//article/div[1]/div[1]/h1/span/text()').extract_first()
        i['action_type'] = response.url.split('/')[-2]
        closing_info_p = response.xpath('//article/div[2]/div/ul[3]/li[1]').extract_first()
        submited_p = closing_info_p.split('<br>')[-1]
        submited_serach = re.search(r'(\d+)', submited_p, re.IGNORECASE)
        if submited_serach:
            print(response.url, submited_serach)
            i['proposal_submitted'] = submited_serach.group(1)
        else:
            i['proposal_submitted'] = None
        logger.info('Got call id %s', i)
