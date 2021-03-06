import scrapy
import logging
from fbcrawl.settings import FB_EMAIL, FB_PASSWORD
from scrapy.loader import ItemLoader
from scrapy.http import FormRequest
from scrapy.exceptions import CloseSpider
from fbcrawl.items import FbcrawlItem, parse_date, parse_date2
from postgres.models import Post
from datetime import datetime
import random
class FacebookSpider(scrapy.Spider):
    '''
    Parse FB pages (needs credentials)
    '''    
    name = 'fb'
    custom_settings = {
        'FEED_EXPORT_FIELDS': ['source','shared_from','date','text', \
                               'reactions','likes','ahah','love','wow', \
                               'sigh','grrr','comments','post_id','url'],
        'DUPEFILTER_CLASS' : 'scrapy.dupefilters.BaseDupeFilter',
    }
    
    def __init__(self, *args, **kwargs):
        #turn off annoying logging, set LOG_LEVEL=DEBUG in settings.py to see more logs
        logger = logging.getLogger('scrapy.middleware')
        logger.setLevel(logging.WARNING)
        
        super().__init__(*args,**kwargs)
        
        #email & pass need to be passed as attributes!
        if FB_EMAIL and FB_PASSWORD:
            self.password = FB_PASSWORD
            self.email = FB_EMAIL
        elif 'email' not in kwargs or 'password' not in kwargs:
            raise AttributeError('You need to provide valid email and password:\n'
                                 'scrapy fb -a email="EMAIL" -a password="PASSWORD"')
        else:
            self.logger.info('Email and password provided, will be used to log in')

        #page name parsing (added support for full urls)
        if 'page' not in kwargs and 'pages' not in kwargs:
            raise AttributeError('Please provide page url')
        if 'page' in kwargs:
            self.pages = self.page
        self.parse_image = False
        if 'image' in kwargs and kwargs['image']:
            self.logger.info('Crawl image as well!')
            self.parse_image = True

        if 'ignore_existed' not in kwargs:
            self.ignore_existed = False
        urls = []
        for url in self.pages.split(','):
            if url.find('/groups/') != -1:
                self.group = 1
            else:
                self.group = 0
            # get sub urls
            for domain in ['https://www.facebook.com/', 'https://mbasic.facebook.com/', 'https://m.facebook.com/']:
                if url.find(domain) != -1:
                    url = url[len(domain):]
                    urls.append(url)
                    break
        random.shuffle(urls)
        self.pages = urls
        #parse date
        if 'date' not in kwargs:
            self.logger.info('Date attribute not provided, scraping date set to 2004-02-04 (fb launch date)')
            self.date = datetime(2004,2,4)
        else:
            self.date = datetime.strptime(kwargs['date'],'%Y-%m-%d')
            self.logger.info('Date attribute provided, fbcrawl will stop crawling at {}'.format(kwargs['date']))
        self.year = self.date.year

        #parse lang, if not provided (but is supported) it will be guessed in parse_home
        if 'lang' not in kwargs:
            self.logger.info('Language attribute not provided, fbcrawl will try to guess it from the fb interface')
            self.logger.info('To specify, add the lang parameter: scrapy fb -a lang="LANGUAGE"')
            self.logger.info('Currently choices for "LANGUAGE" are: "en", "es", "fr", "it", "pt"')
            self.lang = '_'                       
        elif self.lang == 'en'  or self.lang == 'es' or self.lang == 'fr' or self.lang == 'it' or self.lang == 'pt':
            self.logger.info('Language attribute recognized, using "{}" for the facebook interface'.format(self.lang))
        else:
            self.logger.info('Lang "{}" not currently supported'.format(self.lang))                             
            self.logger.info('Currently supported languages are: "en", "es", "fr", "it", "pt"')                             
            self.logger.info('Change your interface lang from facebook settings and try again')
            raise AttributeError('Language provided not currently supported')
        
        #max num of posts to crawl
        if 'max' not in kwargs:
            self.max = int(10e5)
        else:
            self.max = int(kwargs['max'])
    
        #current year, this variable is needed for proper parse_page recursion
        self.k = datetime.now().year
        #count number of posts, used to enforce DFS and insert posts orderly in the csv
        self.count = 0
        self.page_priorty = 0
        
        self.start_urls = ['https://mbasic.facebook.com']    

    def parse(self, response):
        '''
        Handle login with provided credentials
        '''
        yield FormRequest.from_response(
                response,
                formxpath='//form[contains(@action, "login")]',
                formdata={'email': self.email,'pass': self.password},
                callback=self.parse_home
                )
  
    def parse_home(self, response):
        '''
        This method has multiple purposes:
        1) Handle failed logins due to facebook 'save-device' redirection
        2) Set language interface, if not already provided
        3) Navigate to given page 
        '''
        #handle 'save-device' redirection
        if response.xpath("//div/a[contains(@href,'save-device')]"):
            self.logger.info('Going through the "save-device" checkpoint')
            yield FormRequest.from_response(
                response,
                formdata={'name_action_selected': 'dont_save'},
                callback=self.parse_home
                )
        #set language interface
        if self.lang == '_':
            if response.xpath("//input[@placeholder='Search Facebook']"):
                self.logger.info('Language recognized: lang="en"')
                self.lang = 'en'
            elif response.xpath("//input[@placeholder='Buscar en Facebook']"):
                self.logger.info('Language recognized: lang="es"')
                self.lang = 'es'
            elif response.xpath("//input[@placeholder='Rechercher sur Facebook']"):
                self.logger.info('Language recognized: lang="fr"')
                self.lang = 'fr'
            elif response.xpath("//input[@placeholder='Cerca su Facebook']"):
                self.logger.info('Language recognized: lang="it"')
                self.lang = 'it'
            elif response.xpath("//input[@placeholder='Pesquisa no Facebook']"):
                self.logger.info('Language recognized: lang="pt"')
                self.lang = 'pt'
            else:
                raise AttributeError('Language not recognized\n'
                                     'Change your interface lang from facebook ' 
                                     'and try again')
                                                                 
        #navigate to provided page
        for page_url in self.pages:
            self.count = 0
            href = response.urljoin(page_url)
            self.logger.info('Scraping facebook page {}'.format(href))
            yield scrapy.Request(url=href, callback=self.parse_page, meta={'index':1})

    def parse_page(self, response):
        '''
        Parse the given page selecting the posts.
        Then ask recursively for another page.
        '''
#        #open page in browser for debug
#        from scrapy.utils.response import open_in_browser
#        open_in_browser(response)
    
        #select all posts
        for post in response.xpath("//div[contains(@data-ft,'top_level_post_id')]"):     
 
            many_features = post.xpath('./@data-ft').get()
            date = []
            date.append(many_features)
            date = parse_date(date,{'lang':self.lang})
            current_date = datetime.strptime(date,'%Y-%m-%d %H:%M:%S') if date is not None else date
            
            if current_date is None:
                date_string = post.xpath('.//abbr/text()').get()
                date = parse_date2([date_string],{'lang':self.lang})
                current_date = datetime(date.year,date.month,date.day) if date is not None else date   
                date = str(date)
                
            #if 'date' argument is reached stop crawling
            if self.date > current_date:
                raise CloseSpider('Reached date: {}'.format(self.date))
            if abs(self.count) > self.max:
                raise CloseSpider(reason='Reach max post limit')
            new = ItemLoader(item=FbcrawlItem(),selector=post)
            self.logger.info('Parsing post n = {}, post_date = {}'.format(abs(self.count)+1,date))
            new.add_xpath('comments', './div[2]/div[2]/a[1]/text()')     
            new.add_value('date',date)
            new.add_xpath('post_id','./@data-ft')
            new.add_xpath('url', ".//a[contains(@href,'footer')]/@href")
            new.context['lang'] = self.lang
            #page_url #new.add_value('url',response.url)
            self.count -= 1

            check_item = new.load_item()
            if self.ignore_existed and Post.select().where(Post.url == check_item['url']).exists():
                continue
            #returns full post-link in a list
            post = post.xpath(".//a[contains(@href,'footer')]/@href").extract() 
            temp_post = response.urljoin(post[0])
            yield scrapy.Request(temp_post, self.parse_post, priority=0, meta={'item':new})

        #load following page, try to click on "more"
        #after few pages have been scraped, the "more" link might disappears 
        #if not present look for the highest year not parsed yet
        #click once on the year and go back to clicking "more"
        
        #new_page is different for groups
        if self.group == 1:
            new_page = response.xpath("//div[contains(@id,'stories_container')]/div[2]/a/@href").extract()      
        else:
            new_page = response.xpath("//div[2]/a[contains(@href,'timestart=') and not(contains(text(),'ent')) and not(contains(text(),number()))]/@href").extract()      
            #this is why lang is needed                                            ^^^^^^^^^^^^^^^^^^^^^^^^^^               
        
        if not new_page: 
            self.logger.info('[!] "more" link not found, will look for a "year" link')
            #self.k is the year link that we look for 
            if response.meta['flag'] == self.k and self.k >= self.year:                
                xpath = "//div/a[contains(@href,'time') and contains(text(),'" + str(self.k) + "')]/@href"
                new_page = response.xpath(xpath).extract()
                self.page_priorty -= 1
                if new_page:
                    new_page = response.urljoin(new_page[0])
                    self.k -= 1
                    self.logger.info('Found a link for year "{}", new_page = {}'.format(self.k,new_page))

                    yield scrapy.Request(new_page, callback=self.parse_page, priority=self.page_priorty, meta={'flag':self.k})
                else:
                    while not new_page: #sometimes the years are skipped this handles small year gaps
                        self.logger.info('Link not found for year {}, trying with previous year {}'.format(self.k,self.k-1))
                        self.k -= 1
                        if self.k < self.year:
                            raise CloseSpider('Reached date: {}. Crawling finished'.format(self.date))
                        xpath = "//div/a[contains(@href,'time') and contains(text(),'" + str(self.k) + "')]/@href"
                        new_page = response.xpath(xpath).extract()
                    self.logger.info('Found a link for year "{}", new_page = {}'.format(self.k,new_page))
                    new_page = response.urljoin(new_page[0])
                    self.k -= 1
                    yield scrapy.Request(new_page, callback=self.parse_page, priority=self.page_priorty, meta={'flag':self.k}) 
            else:
                self.logger.info('Crawling has finished with no errors!')
        else:
            new_page = response.urljoin(new_page[0])
            self.page_priorty -= 1
            if 'flag' in response.meta:
                self.logger.info('Page scraped, clicking on "more"! new_page = {}'.format(new_page))
                yield scrapy.Request(new_page, callback=self.parse_page, priority=self.page_priorty, meta={'flag':response.meta['flag']})
            else:
                self.logger.info('First page scraped, clicking on "more"! new_page = {}'.format(new_page))
                yield scrapy.Request(new_page, callback=self.parse_page, priority=self.page_priorty, meta={'flag':self.k})

    def parse_post(self, response):
        new = ItemLoader(item=FbcrawlItem(),response=response,parent=response.meta['item'])
        new.context['lang'] = self.lang
        new.add_xpath('source', "//td/div/h3/strong/a/text() | //span/strong/a/text() | //div/div/div/a[contains(@href,'post_id')]/strong/text()")
        new.add_xpath('shared_from','//div[contains(@data-ft,"top_level_post_id") and contains(@data-ft,\'"isShare":1\')]/div/div[3]//strong/a/text()')
     #   new.add_xpath('date','//div/div/abbr/text()')
        new.add_xpath('text','//div[@data-ft]//p//text() | //div[@data-ft]/div[@class]/div[@class]/text()')
        #check reactions for old posts
        check_reactions = response.xpath("//a[contains(@href,'reaction/profile')]/div/div/text()").get()

        if check_reactions: 
            new.add_xpath('reactions',"//a[contains(@href,'reaction/profile')]/div/div/text()")
            reactions = response.xpath("//div[contains(@id,'sentence')]/a[contains(@href,'reaction/profile')]/@href")
            reactions = response.urljoin(reactions[0].extract())
            reaction_payload = {
                'check': True,
                'url': reactions
            }
        else:
            reaction_payload = {
                'check': False,
            }

        image_path = response.xpath('//div[@data-ft]/div[@class]/a/@href')
        if image_path.get() and self.parse_image:
            image_urls = image_path
            img_prefix = '/photo.php'
            found_img_urls = []
            for selected_url in image_urls:
                url = selected_url.extract()
                if img_prefix in url:
                    found_img_urls.append(response.urljoin(url))
            if len(found_img_urls) > 0:
                first_url = found_img_urls.pop()
                yield scrapy.Request(first_url, callback=self.download_image,  
                    meta = {'remaining_images' : found_img_urls, 'item': new, 'img_urls': [], 'check_reactions': reaction_payload })
            else:
                yield new.load_item()
        else:
            new.add_value('image_urls', [])
            if reaction_payload['check']:
                yield scrapy.Request(reaction_payload['url'], callback=self.parse_reactions, meta={'item':new})
            else:
                yield new.load_item()

    def download_image(self, response):
        # full_img_url = response.xpath('//a[contains(text(), "View")')
        img_urls = response.meta['img_urls']
        remaining_images = response.meta['remaining_images']
        new = ItemLoader(item=FbcrawlItem(),response=response, parent=response.meta['item'])
        new.context['lang'] = self.lang

        CDN_DOMAIN_MATCH = 'https://scontent-tpe1-1.xx.fbcdn.net'
        for img_element in response.xpath('//img/@src'):
            img_url = img_element.extract()
            if 'scontent' in img_url:
                 img_urls.append(img_url)
                 break

        if len(remaining_images) > 0:
            img_url = remaining_images.pop()
            yield scrapy.Request(img_url, callback=self.download_image, 
                meta={'remaining_images' : remaining_images, 'item': new, 'img_urls': img_urls, 'check_reactions': response.meta['check_reactions'] })
        else:
            new.add_value('image_urls', img_urls)
            reaction_payload = response.meta['check_reactions']
            if reaction_payload['check']:
                yield scrapy.Request(reaction_payload['url'], callback=self.parse_reactions, meta={'item':new})
            else:
                yield new.load_item()

    def parse_reactions(self,response):
        new = ItemLoader(item=FbcrawlItem(),response=response, parent=response.meta['item'])
        new.context['lang'] = self.lang
        new.add_xpath('likes',"//a[contains(@href,'reaction_type=1')]/span/text()")
        new.add_xpath('ahah',"//a[contains(@href,'reaction_type=4')]/span/text()")
        new.add_xpath('love',"//a[contains(@href,'reaction_type=2')]/span/text()")
        new.add_xpath('wow',"//a[contains(@href,'reaction_type=3')]/span/text()")
        new.add_xpath('sigh',"//a[contains(@href,'reaction_type=7')]/span/text()")
        new.add_xpath('grrr',"//a[contains(@href,'reaction_type=8')]/span/text()")     
        yield new.load_item()