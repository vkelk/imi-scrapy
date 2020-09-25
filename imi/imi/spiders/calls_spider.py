import logging
import re
import scrapy
from scrapy.loader import ItemLoader
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from imi.items import CallItem


logger = logging.getLogger()


def remove_html_tags(text):
    """Remove html tags from a string"""
    clean = re.compile('<.*?>')
    text = re.sub(clean, '', text)
    text = re.sub(re.compile(r'\n'), ' ', text)
    return text.strip()


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
        i['url'] = response.url
        i['call_id'] = response.xpath('//article/div[1]/div[1]/h1/span/text()').extract_first()
        i['action_type'] = response.url.split('/')[-2]
        topics = response.xpath('//article/div[@class="content"]/div/*[contains(., "opic")]/following-sibling::ul[1]/li').extract()
        i['topics'] = [remove_html_tags(t) for t in topics]
        buget = response.xpath('//article/div[@class="content"]/div/*[contains(., "budget")]/following-sibling::ul[1]/li').extract()
        i['indicative_budget'] = [remove_html_tags(b) for b in buget]
        closing_info_p = response.xpath('//article/div[2]/div/ul[3]/li[1]').extract_first()
        submited_p = closing_info_p.split('<br>')[-1]
        submited_serach = re.search(r'(\d+)', submited_p, re.IGNORECASE)
        if submited_serach:
            i['proposal_submitted'] = submited_serach.group(1)
        else:
            i['proposal_submitted'] = None
        launched = response.xpath('//article/div[@class="content"]/div/*[contains(., "launched")]/text()').extract()
        if len(launched) > 0:
            launched_text = ' '.join(launched)
            launched_search = re.search(r'(\d+\s\w+\s+\d+)', launched_text, re.IGNORECASE)
            if launched_search:
                i['call_date'] = launched_search.group(1)
        yield i
