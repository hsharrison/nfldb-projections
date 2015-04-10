import pandas as pd
from scrapy import Field, Item
from scrapy.contrib.spiders import CrawlSpider

positions = ('qb', 'rb', 'wr', 'te', 'k')
column_conversions = {
}


def PlayerRow(**data):
    item = Item()
    for field, value in data.iteritems():
        item.fields[field] = Field()
        item[field] = value
    return item


class FantasyProsSpider(CrawlSpider):
    name = 'fantasypros_spider'
    start_urls = (
        'http://www.fantasypros.com/nfl/projections/{}.php'.format(pos)
        for pos in positions
    )
    allowed_domains = ['fantasypros.com']

    def parse_start_url(self, response):
        position = response.url[43:45].upper()
        data = pd.read_table(response.url + '?export=xls', header=3).iloc[:, :-1].copy()
        data.columns = [column_conversions.get(column, column) for column in data.columns]

        for _, row in data.iterrows():
            item = PlayerRow(pos=position)
            for field, value in row.iteritems():
                item.fields[field] = Field()
                item[field] = value

            yield item
