from itertools import chain, izip

from scrapy import Item, Field
from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.contrib.linkextractors import LinkExtractor

columns_by_pos = {
    'QB': [
        'week',
        'passing_cmp_passing_att',
        'passing_yds',
        'passing_tds',
        'passing_ints',
        'rushing_atts',
        'rushing_yds',
        'rushing_tds',
        'fp_ci',
        'fp',
    ],
    'RB': [
        'week',
        'rushing_atts',
        'rushing_yds',
        'rushing_tds',
        'recs',
        'receiving_yds',
        'receiving_tds',
        'fp_ci',
        'fp',
    ],
    'WR': [
        'week',
        'recs',
        'receiving_yds',
        'receiving_tds',
        'fp_ci',
        'fp',
    ],
    'K': [
        'week',
        'fgm',
        'fga',
        'xpm',
        'fp_ci',
        'fp',
    ],
    'D': [
        'week',
        'defense_pts_against',
        'defense_sk',
        'defense_ints',
        'defense_frec',
        'defense_tds',
        'fp_ci',
        'fp',
    ]
}
columns_by_pos['TE'] = columns_by_pos['WR']
all_fields = set(chain.from_iterable(columns_by_pos.itervalues()))
all_fields.update(['name', 'team', 'pos'])


def PlayerRow(**fields):
    item = Item()
    for field in all_fields:
        item.fields[field] = Field()
    for field, value in fields.iteritems():
        item[field] = value
    return item


class NumberfireSpider(CrawlSpider):
    name = 'numberfire_spider'
    start_urls = [
        'http://www.numberfire.com/nfl/players/',
    ]
    allowed_domains = ['numberfire.com']

    rules = [
        Rule(LinkExtractor(
            allow=r'^http://www\.numberfire\.com/nfl/players/[\w-]+$',
            restrict_xpaths='//*[@id="browse-players"]',
            ), 'parse_player',
        ),
    ]

    def __init__(self, *args, **kwargs):
        super(NumberfireSpider, self).__init__(*args, **kwargs)

    def parse_player(self, response):
        name = response.xpath('//*[@id="player-headline"]/tbody/tr/td[2]/h3/span[1]/text()').extract()[0]
        pos, team = response.xpath('//*[@id="player-headline"]/tbody/tr/td[2]/h3/span[2]/text()').extract()[0]\
            .strip('()').split(',')

        for row in response.xpath('//*[@id="this-week"][2]/table/tbody/tr'):
            player_row = PlayerRow(name=name, pos=pos, team=team)
            for field, value in izip(columns_by_pos[pos], row.xpath('./td/text()').extract()):
                player_row[field] = value
            yield player_row
