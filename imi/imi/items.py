# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class CallItem(scrapy.Item):
    call_id = scrapy.Field()
    action_type = scrapy.Field()
    proposal_submitted = scrapy.Field()
    topics = scrapy.Field()
    indicative_budget = scrapy.Field()
    publication_date = scrapy.Field()
    sub_start_date = scrapy.Field()
    sub_end_date = scrapy.Field()
    call_date = scrapy.Field()
    url = scrapy.Field()


class ProjectItem(scrapy.Item):
    gan = scrapy.Field()
    start_date = scrapy.Field()
    end_date = scrapy.Field()
    call_id = scrapy.Field()
    call_date = scrapy.Field()
    status = scrapy.Field()
    program = scrapy.Field()
    disease_area = scrapy.Field()
    imi_funding = scrapy.Field()
    efpia_inkind = scrapy.Field()
    other = scrapy.Field()
    project_name = scrapy.Field()
    project_intro = scrapy.Field()
    project_website = scrapy.Field()
    twitter_handle = scrapy.Field()
    project_coordinator = scrapy.Field()
    project_leader = scrapy.Field()
    project_manager = scrapy.Field()
    url = scrapy.Field()
    summary = scrapy.Field()
    fundings = scrapy.Field()
    participants = scrapy.Field()
