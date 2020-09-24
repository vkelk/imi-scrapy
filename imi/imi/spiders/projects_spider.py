import logging
import re
import scrapy
from scrapy.loader import ItemLoader
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from imi.items import ProjectItem


logger = logging.getLogger()


def remove_html_tags(text):
    """Remove html tags from a string"""
    clean = re.compile('<.*?>')
    text = re.sub(clean, '', text)
    text = re.sub(re.compile(r'\n'), ' ', text)
    return text.strip()


class CallsSpider(CrawlSpider):
    name = 'projects'
    allowed_domains = ['imi.europa.eu']
    base_url = 'https://imi.europa.eu/'
    start_urls = ['https://www.imi.europa.eu/projects-results/project-factsheets']
    # rules = [Rule(LinkExtractor(allow='catalogue/'), callback='parse_filter_book', follow=True)]
    rules = [
        Rule(LinkExtractor(
            unique=True,
            allow=(r'projects-results\/project-factsheets\/[A-Za-z0-9-]+'),
            # restrict_xpaths=('//article')
            ),
            callback='parse_item', follow=True)
    ]

    def parse_item(self, response):
        i = ProjectItem()
        i['project_name'] = response.xpath('//div[@id="project-title"]/div/div[1]/h1/span/text()').extract_first()
        i['gan'] = response.xpath('//div[@id="project-facts-figures"]/table[1]/tbody/tr[4]/td[2]/div/text()').extract_first()
        i['start_date'] = response.xpath('//article/div/div[2]/div[1]/div[2]/div/table[1]/tbody/tr[1]/td[2]/div/time/text()').extract_first()
        i['end_date'] = response.xpath('//article/div/div[2]/div[1]/div[2]/div/table[1]/tbody/tr[2]/td[2]/div/time/text()').extract_first()
        i['call_id'] = response.xpath('//article/div/div[2]/div[1]/div[2]/div/table[1]/tbody/tr[3]/td[2]/div/text()').extract_first()
        i['call_date'] = ''
        i['status'] = response.xpath('//span[@class="project-status"]/text()').extract_first()
        i['program'] = response.xpath('//span[@class="project-imi-programme"]/text()').extract_first()
        i['disease_area'] = response.xpath('//div[@id="project-tags"]//a[@class="project-keyword"]/text()').getall()
        i['imi_funding'] = response.xpath('//div[contains(@class, "field--name-field-funding-imi")]/@content').extract_first()
        i['efpia_inkind'] = response.xpath('//div[contains(@class, "field--name-field-funding-efpi")]/@content').extract_first()
        i['other'] = response.xpath('//div[contains(@class, "field--name-field-funding-other")]/@content').extract_first()
        i['project_intro'] = response.xpath('//article/div/div[2]/div[2]/div[1]/div/div[1]/div/text()').extract_first()
        i['project_website'] = response.xpath('//article/div/div[2]/div[1]/div[3]/div/p[1]/a/text()').extract_first()
        i['twitter_handle'] = response.xpath('//article/div/div[2]/div[1]/div[3]/div/p[2]/a/text()').extract_first()
        i['project_coordinator'] = response.xpath('//div[@id="project-contacts"]/div/div[@class="field--item"]/div[@class="project-contact"][contains(strong, "Project coordinator")]/text()').extract()
        i['project_leader'] = response.xpath('//div[@id="project-contacts"]/div/div[@class="field--item"]/div[@class="project-contact"][contains(strong, "Project leader")]/text()').extract()
        i['project_manager'] = response.xpath('//div[@id="project-contacts"]/div/div[@class="field--item"]/div[@class="project-contact"][contains(strong, "Project Manager")]/text()').extract()
        i['url'] = response.url
        i['summary'] = remove_html_tags(response.xpath('//*[@id="project-body"]/div/*').extract()[0])
        i['fundings'] = response.xpath('//article/div/div[2]/div[2]/div[4]/div[2]/div[2]/div/div/div[2]/div/div/table/tbody/*').extract()
        i['participants'] = response.xpath('//div[@class="project-participants-category"]/*').extract()
        logger.info('Got item %s', i)
        yield i
